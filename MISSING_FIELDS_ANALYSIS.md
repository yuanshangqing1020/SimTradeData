# SimTradeData Valuation 表缺失字段分析报告

## 1. 问题确认
经过对 DuckDB 数据库 `valuation` 表的检查，确认以下字段目前全部为空（NULL）：
- **财务指标**: `roe` (净资产收益率), `roe_ttm`, `roa` (总资产收益率), `roa_ttm`, `naps` (每股净资产)
- **股本信息**: `total_shares` (总股本), `a_floats` (流通股本)

目前仅以下字段有数据：
- `pe_ttm` (市盈率TTM)
- `pb` (市净率)
- `ps_ttm` (市销率TTM)
- `pcf` (市现率)
- `turnover_rate` (换手率)

## 2. 原因分析

### 2.1 数据源限制
目前 `valuation` 表的数据主要来源于 BaoStock 的日线估值接口 (`query_history_k_data_plus`)。该接口**仅提供** PE、PB、PS、PCF 和换手率等基于价格的每日指标，**不包含** 股本（Shares）和 财务比率（ROE/ROA）等数据。

### 2.2 数据处理流程缺失
虽然 `fundamentals` 表（来源于 Mootdx 季报数据）中包含了 `total_shares`, `a_floats`, `roe`, `roa` 等完整数据，但目前的下载脚本 (`scripts/download.py`) 和导出脚本 (`scripts/export_parquet.py`) **缺少将季度财务数据“填充”或“对齐”到日线估值表（Valuation）的 ETL 逻辑**。

SimTradeData 的架构中，`valuation` 表被设计为一张宽表，但在实现上尚未完成从 `fundamentals` 到 `valuation` 的数据同步。

## 3. 对 SimTradeLab 的影响分析

这些字段的缺失会对 `SimTradeLab` 的回测和交易功能产生以下具体影响：

### 3.1 市值计算失效 (严重)
在 `SimTradeLab` 的 `api.py` 中，获取市值 (`total_value`) 和流通市值 (`float_value`) 的逻辑依赖于 `valuation` 表中的股本数据：

```python
# src/simtradelab/ptrade/api.py
total_shares = row.get('total_shares')
if total_shares is not None ...:
    stock_data[field] = close_prices[stock] * total_shares  # 实时计算市值
```

由于 `total_shares` 和 `a_floats` 为空，**`get_fundamentals` 查询市值将返回 NaN 或无效值**。这会导致：
- **小市值策略失效**：依赖市值过滤股票的策略将无法选出任何股票。
- **权重计算错误**：依赖市值加权的指数或组合构建逻辑将失败。

### 3.2 财务因子选股受限
如果策略代码习惯从 `valuation` 表（通常作为日频数据入口）获取 `roe` 或 `roa` 用于选股或排序，将获取不到数据。
*注：如果策略是从 `fundamentals` 表获取这些数据，则不受影响，但通常 PTrade 用户习惯在 `valuation` 中获取合成后的 TTM 数据。*

### 3.3 PB/PE 策略不受影响
依赖纯估值指标（如低 PE、低 PB 策略）的功能仍然正常，因为 `pe_ttm` 和 `pb` 字段是有数据的。

## 4. 建议解决方案

建议在**不修改现有下载逻辑**的前提下，在**数据导出阶段**（`export_parquet.py`）或**数据入库后处理阶段**进行修复：

1.  **实现数据合并 (Merge)**：
    在导出 Parquet 之前，读取 `valuation` 表和 `fundamentals` 表。
2.  **前向填充 (Forward Fill)**：
    对于 `valuation` 表中的每一天，匹配最近一个已披露的季度财报（`fundamentals`），将 `total_shares`, `a_floats`, `roe` 等字段填充进去。
3.  **计算 TTM 指标**:
    利用财报数据计算每日的动态 TTM 指标（如需）。

此方案可以确保生成的 Parquet 文件符合 `SimTradeLab` 的预期，恢复市值计算功能。

## 5. 技术修复方案 (Technical Implementation Plan)

经过代码审查，发现 `simtradedata/writers/duckdb_writer.py` 中的 `_export_valuation_enriched` 方法存在逻辑错误，导致数据关联失败。

### 5.1 当前问题逻辑
```sql
LEFT JOIN quarterly_data q ON d.date = q.date
```
- **错误点 1 (关联键错误)**: 试图将 `valuation` 的**交易日期** (`d.date`) 直接与 `fundamentals` 的**财报期末日期** (`q.date`, 如 3月31日) 进行等值连接。这两个日期在绝大多数情况下不相等。
- **错误点 2 (忽略披露日)**: 即使日期相等，使用财报期末日期也会导致**未来函数** (Lookahead Bias)。因为财报通常在期末后 1-4 个月才披露 (`publ_date`)。

### 5.2 修复思路
利用 DuckDB 的 `ASOF JOIN` 功能，根据**披露日期** (`publ_date`) 进行非等值关联。

**修正后的 SQL 逻辑**:
1.  **清洗 Fundamentals**: 提取 `publ_date` 并转换为 DATE 类型（原为 VARCHAR 'YYYYMMDD'）。
2.  **ASOF JOIN**: 将 `valuation` 表作为左表，`fundamentals` 表作为右表。
3.  **匹配条件**: `v.symbol = f.symbol` 且 `v.date >= f.publ_date`。
4.  **取值**: 取最近一个披露日的数据。

### 5.3 拟定修改代码 (duckdb_writer.py)

```python
    def _export_valuation_enriched(self, symbol_escaped: str, output_file: Path) -> None:
        """
        Export valuation data with enriched fields using ASOF JOIN on publ_date
        """
        self.conn.execute(f"""
            COPY (
                WITH fund_data AS (
                    SELECT
                        -- Convert YYYYMMDD string to DATE
                        TRY_CAST(strptime(publ_date, '%Y%m%d') AS DATE) as match_date,
                        roe, roa, roe_ttm, roa_ttm,
                        total_shares, a_floats
                    FROM fundamentals
                    WHERE symbol = '{symbol_escaped}' 
                      AND publ_date IS NOT NULL 
                      AND publ_date != ''
                ),
                val_data AS (
                    SELECT
                        date,
                        pe_ttm, pb, ps_ttm, pcf, turnover_rate,
                        -- Get close for naps calculation
                        (SELECT close FROM stocks s WHERE s.symbol = '{symbol_escaped}' AND s.date = v.date) as close
                    FROM valuation v
                    WHERE symbol = '{symbol_escaped}'
                )
                SELECT
                    v.date,
                    v.pe_ttm, v.pb, v.ps_ttm, v.pcf,
                    f.roe, f.roe_ttm, f.roa, f.roa_ttm,
                    CASE 
                        WHEN v.pb > 0 AND v.close IS NOT NULL THEN ROUND(v.close / v.pb, 4) 
                        ELSE NULL 
                    END AS naps,
                    f.total_shares,
                    f.a_floats,
                    v.turnover_rate
                FROM val_data v
                ASOF JOIN fund_data f ON v.date >= f.match_date
                ORDER BY v.date
            ) TO '{output_file}' (FORMAT PARQUET)
        """)
```

### 5.4 验证计划
1.  修改 `duckdb_writer.py`。
2.  重新运行 `scripts/export_parquet.py`。
3.  检查生成的 Parquet 文件中 `total_shares` 等字段是否已填充数据。
