# Mootdx 下载报错 'datetime' 原因分析

## 问题描述
在运行 `scripts/download.py` (使用 mootdx 源) 时，日志中出现大量如下错误：
```
ERROR - Failed to fetch daily bars for 000041.SZ: 'datetime'
ERROR - Failed to download 000041.SZ: 'datetime'
```
该错误导致部分股票数据下载失败，且错误日志大量刷屏。

## 原因分析

### 1. 错误来源
通过错误追踪和复现脚本，确认该错误并非来自 `SimTradeData` 的代码，而是直接源自第三方库 `mootdx` 的内部实现。

具体报错位置：
`site-packages/mootdx/quotes.py` 的 `get_k_data` 方法中：
```python
data = data.assign(date=data['datetime'].apply(lambda x: str(x)[0:10])).assign(code=str(code))
```
当 `mootdx` 从服务器获取数据解析后，得到的 DataFrame `data` 中缺少 `datetime` 列，导致访问 `data['datetime']` 时抛出 `KeyError: 'datetime'`。

### 2. 触发场景
该错误主要发生在部分特定股票上，例如：
- `000033.SZ` (*ST新都 - 已退市)
- `000041.SZ` (*ST长生 - 已退市)
- `000043.SZ` (中航善达 - 代码变更/退市)
- `000044.SZ` (深科苑 - 已退市)

这些股票通常是**已退市**或**长期停牌/无数据**的股票。当请求这些股票的数据时，TDX 服务器可能返回空数据或异常数据包，`mootdx` 库在解析时未能正确处理这种空状态，直接尝试进行列操作，从而导致崩溃。

### 3. 合理性评估
- **现象合理性**：这是第三方库在处理边缘情况（无数据/退市股票）时的健壮性不足导致的，属于已知类型的外部库 Bug。
- **数据影响**：由于这些股票本身大多已退市或无有效交易数据，下载失败对策略回测（尤其是针对当前上市股票的策略）影响极小。
- **系统影响**：目前的异常处理机制虽然捕获了该错误（未导致整个脚本中断），但将其记录为 ERROR 级别，造成日志干扰。

## 解决方案建议

建议修改 `simtradedata/fetchers/mootdx_fetcher.py` 中的 `fetch_daily_bars` 方法，增加对 `KeyError` 的捕获。

**修改前逻辑**：
```python
try:
    df = self._client.k(...)
except ValueError as e:
    # ... 处理 "No objects to concatenate"
except Exception as e:
    logger.error(...) # 记录所有其他错误
    raise
```

**建议修改逻辑**：
显式捕获 `KeyError` 并检查是否为 `'datetime'`，如果是则视为无数据处理，返回空 DataFrame，不再报错。

```python
except KeyError as e:
    if "'datetime'" in str(e):
        logger.debug(f"No data for {symbol} (mootdx returned invalid format/missing datetime)")
        return pd.DataFrame()
    logger.error(f"Failed to fetch daily bars for {symbol}: {e}")
    raise
```
这样既能屏蔽无效的错误日志，又能保证程序的正常运行。
