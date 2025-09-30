# BaoStock API 快速参考

> **完整文档**: [BaoStock_API_Reference.md](./BaoStock_API_Reference.md)
> **官方网站**: http://baostock.com

---

## 快速开始

### 安装
```bash
pip install baostock
```

### 基本流程
```python
import baostock as bs
import pandas as pd

# 1. 登录
lg = bs.login()

# 2. 查询数据
rs = bs.query_history_k_data_plus("sh.600000", ...)

# 3. 处理结果
data_list = []
while (rs.error_code == '0') & rs.next():
    data_list.append(rs.get_row_data())
df = pd.DataFrame(data_list, columns=rs.fields)

# 4. 登出
bs.logout()
```

---

## 核心 API 索引

### 1. 系统管理
| 函数 | 说明 |
|------|------|
| `bs.login()` | 登录系统 |
| `bs.logout()` | 登出系统 |

### 2. 历史行情数据
| 函数 | 说明 | 返回字段示例 |
|------|------|------------|
| `query_history_k_data_plus()` | 获取K线数据（日/周/月/分钟） | date, code, open, high, low, close, volume, amount, adjustflag, turn, pctChg, peTTM, pbMRQ |

**K线数据参数**:
- `code`: 股票代码（如 "sh.600000"）
- `fields`: 指标字符串（逗号分隔）
- `start_date`: 开始日期 "YYYY-MM-DD"
- `end_date`: 结束日期 "YYYY-MM-DD"
- `frequency`: 频率 "d"日/"w"周/"m"月/"5"5分钟/"15"15分钟/"30"30分钟/"60"60分钟
- `adjustflag`: 复权类型 "1"后复权/"2"前复权/"3"不复权

### 3. 除权除息
| 函数 | 说明 |
|------|------|
| `query_dividend_data()` | 查询除权除息信息 |
| `query_adjust_factor()` | 查询复权因子 |

### 4. 季频财务数据
| 函数 | 说明 | 主要指标 |
|------|------|---------|
| `query_profit_data()` | 季频盈利能力 | roeAvg, npMargin, gpMargin, netProfit, epsTTM, MBRevenue, totalShare, liqaShare |
| `query_operation_data()` | 季频营运能力 | assetTurnover, inventoryTurnover, receivablesTurnover, totalAssetTurnover, dupontROE |
| `query_growth_data()` | 季频成长能力 | YOYEquity, YOYAsset, YOYNI, YOYEPSBasic, YOYPNI, YOYNetProfit |
| `query_balance_data()` | 季频偿债能力 | currentRatio, quickRatio, cashRatio, YOYLiability, liabilityToAsset, assetToEquity |
| `query_cash_flow_data()` | 季频现金流量 | CAToAsset, NCAToAsset, tangibleAsset, ebitToInterest, CFOToOR |
| `query_dupont_data()` | 季频杜邦指数 | dupontROE, dupontAssetStoEquity, dupontAssetTurn, dupontPnitoni, dupontNitogr, dupontTaxBurden, dupontIntburden, dupontEbittogr |

**财务数据参数**:
- `code`: 股票代码
- `year`: 年份
- `quarter`: 季度（1-4）

### 5. 业绩报告
| 函数 | 说明 |
|------|------|
| `query_performance_express_report()` | 季频业绩快报 |
| `query_forecast_report()` | 季频业绩预告 |

### 6. 证券元信息
| 函数 | 说明 | 返回字段 |
|------|------|---------|
| `query_trade_dates()` | 交易日查询 | calendar_date, is_trading_day |
| `query_all_stock()` | 证券代码查询 | code, code_name |
| `query_stock_basic()` | 证券基本资料 | code, code_name, ipoDate, outDate, type, status |
| `query_stock_industry()` | 行业分类 | code, code_name, industry, industryClassification |

### 7. 板块数据
| 函数 | 说明 |
|------|------|
| `query_sz50_stocks()` | 上证50成分股 |
| `query_hs300_stocks()` | 沪深300成分股 |
| `query_zz500_stocks()` | 中证500成分股 |

**板块参数**:
- `date`: 查询日期 "YYYY-MM-DD"

### 8. 宏观经济数据
| 函数 | 说明 |
|------|------|
| `query_deposit_rate_data()` | 存款利率 |
| `query_loan_rate_data()` | 贷款利率 |
| `query_required_reserve_ratio_data()` | 存款准备金率 |
| `query_money_supply_data_month()` | 货币供应量（月） |
| `query_money_supply_data_year()` | 货币供应量（年底余额） |
| `query_shibor_data()` | 银行间同业拆放利率 |

---

## 常用代码模式

### 1. 获取日K线数据
```python
rs = bs.query_history_k_data_plus(
    "sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
    start_date='2023-01-01',
    end_date='2023-12-31',
    frequency="d",
    adjustflag="3"  # 不复权
)
```

### 2. 获取所有股票列表
```python
rs = bs.query_all_stock(day='2023-12-31')
stock_list = []
while (rs.error_code == '0') & rs.next():
    stock_list.append(rs.get_row_data())
stocks_df = pd.DataFrame(stock_list, columns=rs.fields)
```

### 3. 获取财务数据
```python
# 盈利能力
rs = bs.query_profit_data(code="sh.600000", year=2023, quarter=4)

# 现金流量
rs = bs.query_cash_flow_data(code="sh.600000", year=2023, quarter=4)
```

### 4. 获取交易日历
```python
rs = bs.query_trade_dates(start_date="2023-01-01", end_date="2023-12-31")
```

### 5. 批量获取数据
```python
# 获取所有股票的某日K线
stocks_rs = bs.query_all_stock(day='2023-12-31')
stock_list = []
while (stocks_rs.error_code == '0') & stocks_rs.next():
    stock_list.append(stocks_rs.get_row_data())

all_data = []
for stock in stock_list:
    code = stock[0]
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,volume",
        start_date='2023-12-31',
        end_date='2023-12-31',
        frequency="d"
    )
    while (rs.error_code == '0') & rs.next():
        all_data.append(rs.get_row_data())
```

---

## 数据字段说明

### K线数据主要字段
| 字段 | 说明 | 单位 |
|------|------|------|
| date | 交易日期 | YYYY-MM-DD |
| code | 股票代码 | - |
| open | 开盘价 | 元 |
| high | 最高价 | 元 |
| low | 最低价 | 元 |
| close | 收盘价 | 元 |
| preclose | 前收盘价 | 元 |
| volume | 成交量 | 股 |
| amount | 成交额 | 元 |
| adjustflag | 复权状态 | 1后复权/2前复权/3不复权 |
| turn | 换手率 | % |
| tradestatus | 交易状态 | 1正常/0停牌 |
| pctChg | 涨跌幅 | % |
| peTTM | 滚动市盈率 | - |
| pbMRQ | 市净率 | - |
| psTTM | 滚动市销率 | - |
| pcfNcfTTM | 滚动市现率 | - |
| isST | 是否ST | 1是/0否 |

### 财务数据主要字段

**盈利能力**:
- roeAvg: 净资产收益率(平均)(%)
- npMargin: 销售净利率(%)
- gpMargin: 销售毛利率(%)
- netProfit: 净利润(元)
- epsTTM: 每股收益(元)

**偿债能力**:
- currentRatio: 流动比率
- quickRatio: 速动比率
- cashRatio: 现金比率
- liabilityToAsset: 资产负债率(%)

**营运能力**:
- assetTurnover: 总资产周转率(次)
- inventoryTurnover: 存货周转率(次)
- receivablesTurnover: 应收账款周转率(次)

**成长能力**:
- YOYEquity: 净资产同比增长率(%)
- YOYAsset: 总资产同比增长率(%)
- YOYNI: 净利润同比增长率(%)
- YOYEPSBasic: 基本每股收益同比增长率(%)

---

## 注意事项

1. **必须登录登出**: 所有查询前必须调用 `bs.login()`，结束后调用 `bs.logout()`
2. **股票代码格式**: 必须带市场前缀，如 "sh.600000"（上海）、"sz.000001"（深圳）
3. **日期格式**: 统一使用 "YYYY-MM-DD" 格式
4. **数据时间范围**: K线数据从1990-12-19至今，财务数据从2007年开始
5. **结果集遍历**: 使用 `while (rs.error_code == '0') & rs.next()` 模式遍历所有数据
6. **错误处理**: 检查 `rs.error_code` 和 `rs.error_msg` 了解查询状态
7. **频率限制**: 建议控制查询频率，避免对服务器造成压力

---

**版本**: v1.0
**维护者**: SimTradeData Team
**最后更新**: 2025-09-30
