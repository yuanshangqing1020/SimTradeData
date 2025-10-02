# QStock API 快速索引

本文档是 QStock API 的快速查询索引。完整文档请参考：[QStock_API_Reference.md](./QStock_API_Reference.md)

## 📚 核心功能模块

### 1. 实时行情数据
- `realtime_data(market='stock')` - 获取某市场所有标的最新行情
- `get_realtime(code)` - 获取个股最新行情指标
- `get_deal_detail(code)` - 获取日内成交数据
- `get_snapshot(code)` - 获取个股实时交易快照
- `get_changes()` - 实时交易盘口异动数据

### 2. 历史行情数据
- `get_data(code, start='', end='', klt=101, fqt=1)` - 获取历史K线数据
  - `klt`: 1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日, 102=周, 103=月
  - `fqt`: 0=不复权, 1=前复权, 2=后复权

### 3. 股票基本面数据
- `get_basics()` - 获取股票基本信息
- `get_companys(code)` - 获取公司概况
- `get_financial_abstract(code)` - 获取业绩快报摘要

### 4. 财务报表
- `get_balance_sheet(code)` - 获取资产负债表（110+科目）
- `get_income_statement(code)` - 获取利润表
- `get_cash_flow(code)` - 获取现金流量表

### 5. 概念板块数据
- `get_concept_names()` - 获取所有概念板块名称
- `get_concept_stocks(concept_name)` - 获取概念板块成分股
- `get_stock_concepts(code)` - 获取个股所属概念

### 6. 行业板块数据
- `get_industry_names()` - 获取行业分类
- `get_industry_stocks(industry)` - 获取行业成分股

### 7. 资金流数据
- `get_money_flow(code)` - 获取个股资金流向
- `get_hot_rank_concept()` - 概念板块资金流排行

### 8. 宏观经济指标
- 提供多种宏观经济数据接口

### 9. 财经新闻
- 提供财经新闻和文本数据接口

## 🔍 重要说明

### 数据来源
QStock 内部使用 **东方财富网(eastmoney.com)** API 获取数据，已在源码中验证：
```bash
grep -rn "eastmoney" qstock/
# 发现 70 处引用，主要在 data/fundamental.py 等文件
```

### 市场代码
- A股市场: `market='stock'`
- 期货: `market='futures'`
- 概念板块: `market='concept'`
- ETF: `market='etf'`
- 港股: `market='hk'`
- 美股: `market='us'`

### 复权类型
- `fqt=0`: 不复权
- `fqt=1`: 前复权（默认）
- `fqt=2`: 后复权

## 📊 在 SimTradeData 中的应用

### 数据源优先级
```python
# OHLCV行情数据
'get_history': ['BaoStock', 'Mootdx', 'QStock']

# 概念板块数据（QStock 优先）
'get_concept': ['QStock', 'Mootdx']

# ETF数据
'get_etf_info': ['QStock', 'BaoStock']

# 国际市场数据
'港股/美股': ['QStock']
```

### 财务报表覆盖
QStock 提供 **110+ 详细科目**，可以补充 BaoStock 的基础指标：
- 资产负债表: 110+ 科目
- 利润表: 完整科目
- 现金流量表: 完整科目

### 使用建议
1. **A股 OHLCV**: 优先使用 BaoStock/Mootdx（数据质量更稳定）
2. **概念板块**: 优先使用 QStock（同花顺概念最全）
3. **财务报表详细科目**: 使用 QStock 补充 BaoStock 基础指标
4. **国际市场**: 使用 QStock（港股/美股支持）

## 📝 版本信息
- 当前版本: 1.3.8+
- 安装: `pip install qstock`
- 更新: `pip install --upgrade qstock`

## 🔗 参考资源
- GitHub: https://github.com/tkfy920/qstock
- PyPI: https://pypi.org/project/qstock/
- 完整文档: [QStock_API_Reference.md](./QStock_API_Reference.md)
