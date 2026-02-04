# SimTradeData 问题分析报告

## 1. 问题概述

用户在使用 SimTradeData 下载数据并导出 Parquet 文件时遇到两个问题：
1. **字段缺失**：导出生成的 `data/parquet/stocks/000001.SZ.parquet` 文件中，`high_limit`（涨停价）、`low_limit`（跌停价）、`preclose`（昨收价）字段全为空。
2. **起始日期不符**：在 `download_efficient.py` 中配置 `START_DATE = "2015-01-01"`，但下载的 `000001.SZ` 数据实际从 `2016-01-04` 开始。

---

## 2. 问题分析

### 2.1 字段缺失问题 (high_limit, low_limit, preclose 为空)

**根本原因**：
数据来源 `Mootdx`（通达信）的日线数据接口不包含 `preclose`（昨收价）字段，导致数据库中该字段为空，进而导致依赖该字段计算的涨跌停价为空。

**详细分析**：
1. **数据流向**：
   - 系统默认配置下（`scripts/download.py`），OHLCV（开高低收量）行情数据由 `Mootdx` 下载。
   - `BaoStock` 仅在 `valuation_only=True` 模式下运行，只负责下载估值数据（PE/PB等），不写入行情数据到 `stocks` 表。

2. **代码逻辑**：
   - **下载环节**：`simtradedata/fetchers/mootdx_fetcher.py` 中，`fetch_daily_bars` 调用 `mootdx` 接口获取数据。该接口返回字段包括 `date, open, high, low, close, volume, amount`，但**不包含** `preclose`。
   - **入库环节**：`simtradedata/writers/duckdb_writer.py` 的 `write_market_data` 方法将数据写入 `stocks` 表。由于输入 DataFrame 缺少 `preclose`，DuckDB 插入时该字段默认为 NULL。
   - **导出环节**：`simtradedata/writers/duckdb_writer.py` 的 `_export_stocks_with_limits` 方法在导出 Parquet 时，直接读取 `stocks` 表中的 `preclose` 字段来计算 `high_limit` 和 `low_limit`：
     ```sql
     ROUND(preclose * 1.10, 2) AS high_limit
     ```
     因为 `preclose` 为 NULL，所以计算结果也为 NULL。

**解决方案建议**：
在导出 Parquet 环节（`duckdb_writer.py`），利用 SQL 窗口函数动态计算 `preclose`。
- 逻辑：`preclose` 等于上一交易日的 `close`。
- SQL 实现：`LAG(close) OVER (ORDER BY date)`。
- 优点：无需修改下载逻辑，无需重新下载数据，直接修复导出结果。

### 2.2 起始日期不符问题 (数据从 2016 而非 2015 开始)

**根本原因**：
`Mootdx` 使用的免费通达信行情服务器（TDX Server）存在历史数据回溯限制，目前仅能提供最近约 10 年的数据（2016年初至今）。

**详细分析**：
1. **验证测试**：
   - 我们编写了测试脚本 `debug_mootdx.py`，尝试从 `Mootdx` 获取 `000001.SZ` 和 `600000.SS` 在 `2014-01-01` 至 `2015-12-31` 的数据。
   - **结果**：返回数据均为空。
   - 再次请求 `2015-01-01` 至 `2016-01-31`，返回的数据均从 `2016-01-04` 开始。

2. **配置检查**：
   - 虽然 `download_efficient.py` 和 `download_mootdx.py` 中定义了 `START_DATE = "2015-01-01"`，但由于数据源本身没有 2015 年的数据，因此实际落库的数据只能从 2016 年开始。
   - 这是一个数据源服务端的限制，而非代码逻辑错误。

**解决方案建议**：
如果必须获取 2015 年的数据，建议补充使用 `BaoStock` 全量下载模式。
- 方法：运行 `poetry run python scripts/download.py --source baostock --baostock-full`。
- 说明：`BaoStock` 提供更久远的历史数据（通常可回溯至 2006 年甚至更早），可以填补 `Mootdx` 缺失的早期数据。
- 注意：`BaoStock` 的复权因子精度可能不如 `Mootdx`，建议仅用于补全早期历史数据。

---

## 3. 修复计划 (针对字段缺失问题)

既然用户主要关注的是生成的 Parquet 文件质量，我们建议优先修复 **字段缺失** 问题。

**修改文件**：`simtradedata/writers/duckdb_writer.py`

**修改内容**：
在 `_export_stocks_with_limits` 方法中，修改 SQL 查询逻辑：
1. 使用 `LAG(close) OVER (PARTITION BY symbol ORDER BY date)` 计算 `calc_preclose`。
2. 优先使用数据库中已有的 `preclose`（如果有），否则使用 `calc_preclose`。
3. 基于最终的 `preclose` 计算 `high_limit` 和 `low_limit`。

**预期效果**：
- 除每只股票上市首日（或数据起始日）外，所有交易日的 `preclose`、`high_limit`、`low_limit` 将自动填充正确数值。
- 无需重新下载数据。

---

## 4. 总结

| 问题 | 原因 | 建议方案 |
| :--- | :--- | :--- |
| **字段为空** | Mootdx 源不提供 `preclose`，导致库中该字段为 NULL | 修改导出逻辑，使用 SQL 窗口函数根据 `close` 动态计算 `preclose` |
| **日期缺失** | 免费 TDX 服务器仅保留最近约 10 年数据 | 如需 2016 年前数据，请使用 `BaoStock` 全量模式补充下载 |
