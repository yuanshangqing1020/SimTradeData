# SimTradeData - Trading Simulation Data Library

> 🎯 **Data Support for SimTradeLab & SimTradeML** | 📊 **High-Quality Financial Data** | 🚀 **Production Ready**

**[中文文档](README_CN.md)** | **[English](README.md)**

**SimTradeData** is a companion data library for [SimTradeLab](https://github.com/ykayz/SimTradeLab) and [SimTradeML](https://github.com/ykayz/SimTradeML) projects. It provides high-quality financial data to support quantitative strategy backtesting, model development, and performance evaluation.

## 🎯 Core Value

- **📦 Designed for Trading Simulation** - Optimized for quantitative strategy backtesting and model training
- **🔄 Intelligent Data Sync** - Automated historical backfill, incremental updates, gap detection & repair
- **🎨 Zero-Redundancy Architecture** - Carefully designed database structure for efficient storage and fast queries
- **📊 Multi-Source Fusion** - Intelligent switching between BaoStock, Mootdx, and QStock for data availability
- **⚡ Production-Grade Performance** - Cache optimization, concurrent processing for large-scale queries

## 🚀 Quick Start

### 1. Install Dependencies
```bash
# Clone the project
git clone git@github.com:ykayz/SimTradeData.git
cd SimTradeData

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### 2. Initialize Database
```bash
# Create database and table structure
poetry run python scripts/init_database.py --db-path data/simtradedata.db
```

### 3. Sync Data
```bash
# Sync historical data for specific stocks
poetry run python -m simtradedata full-sync --symbols 000001.SZ --target-date 2024-01-01

# Incremental update
poetry run python -m simtradedata incremental --start-date 2024-01-01 --end-date 2024-01-31

# Detect and fix data gaps
poetry run python -m simtradedata gap-fix --start-date 2024-01-01
```

### 4. Use in Code
```python
from simtradedata.database.manager import DatabaseManager
from simtradedata.api.router import APIRouter
from simtradedata.config.manager import Config

# Initialize core components
config = Config()
db_manager = DatabaseManager("data/simtradedata.db")
api_router = APIRouter(db_manager, config)

# Query stock data
data = api_router.get_history(
    symbols=["000001.SZ"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    frequency="1d"
)

# Use in SimTradeLab backtesting
# (See SimTradeLab documentation for details)
```

### 5. Run Tests ✅
```bash
# Run all tests (100% pass rate)
poetry run pytest

# Run quick tests (all important features)
poetry run pytest -m "not slow"

# Run specific test types
poetry run pytest -m sync          # Sync functionality tests
poetry run pytest -m integration   # Integration tests
poetry run pytest -m performance   # Performance tests
```

**Test Results**: ✅ 466 test cases, 100% pass rate

## 📚 Documentation

| Document | Description | Audience | Status |
|----------|-------------|----------|--------|
| [Architecture_Guide.md](docs/Architecture_Guide.md) | Complete architecture design guide | Architects, Developers | ✅ Latest |
| [DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md) | Developer guide | Developers | ✅ Latest |
| [API_REFERENCE.md](docs/API_REFERENCE.md) | API reference | Developers | ✅ Latest |
| [CLI_USAGE_GUIDE.md](docs/CLI_USAGE_GUIDE.md) | CLI usage guide | DevOps | ✅ Latest |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Production deployment guide | DevOps | ✅ Latest |

### 📋 Technical Documentation
| Document | Description | Status |
|----------|-------------|--------|
| [Architecture_Guide.md](docs/Architecture_Guide.md) | Architecture design & implementation | ✅ Complete |

### 📖 Data Source References
| Document | Description | Status |
|----------|-------------|--------|
| [QStock API Reference](docs/reference/qstock_api/QStock_API_Reference.md) | Complete QStock API documentation | ✅ Latest |
| [QStock API Index](docs/reference/qstock_api/QStock_API_Index.md) | QStock quick reference | ✅ Latest |
| [BaoStock API Reference](docs/reference/baostock_api/BaoStock_API_Reference.md) | Complete BaoStock API documentation | ✅ Latest |
| [Mootdx API Reference](docs/reference/mootdx_api/MOOTDX_API_Reference.md) | Complete Mootdx API documentation | ✅ Latest |

> 📋 **Archived Documents**: Historical design docs and research reports moved to [docs/archive/](docs/archive/)

## 💼 Use Cases

### In SimTradeLab

SimTradeData provides complete historical data support for SimTradeLab:

```python
# SimTradeLab strategy backtesting example
from simtradedata.api import APIRouter

# Get historical data for backtesting
api_router = APIRouter(db_manager, config)
backtest_data = api_router.get_history(
    symbols=["000001.SZ", "600000.SS"],
    start_date="2023-01-01",
    end_date="2023-12-31",
    frequency="1d"
)

# Pass to SimTradeLab strategy engine for backtesting
# (See SimTradeLab documentation for details)
```

### In SimTradeML

Provide training and validation data for machine learning models:

```python
# Get data for feature engineering
from simtradedata.sync import SyncManager

# Ensure data completeness
sync_manager = SyncManager(db_manager, data_source_manager)
sync_manager.historical_backfill(symbol="000001.SZ", target_date="2020-01-01")

# Get data for model training
training_data = api_router.get_history(
    symbols=["000001.SZ"],
    start_date="2020-01-01",
    end_date="2023-12-31",
    frequency="1d"
)
```

### Standalone Use

As an independent financial data management tool:

```bash
# Regularly sync latest data
poetry run python -m simtradedata incremental --start-date $(date -d '7 days ago' +%Y-%m-%d)

# Monitor data quality
poetry run python -m simtradedata status

# Fix historical data gaps
poetry run python -m simtradedata gap-fix --start-date 2023-01-01
```

## 🎯 Core Features

### Data Management
- **Intelligent Sync** - Incremental updates, historical backfill, gap detection & repair
- **Multi-Source** - Automatic switching between BaoStock, Mootdx, QStock
- **Data Validation** - Completeness check, quality scoring, anomaly detection
- **Resume Support** - Resume sync after interruption

### Architecture Advantages
- **Zero-Redundancy Design** - Completely eliminate data duplication, unique storage location for each field
- **High-Performance Queries** - Optimized table structure and indexes, 2-5x query speed improvement
- **Intelligent Caching** - Multi-level cache strategy, 434x performance boost for technical indicators
- **Modular Design** - Clear functional separation, easy to maintain and extend

### Monitoring & Operations
- **Data Quality Monitoring** - Real-time monitoring of data source quality and reliability
- **Alert System** - 6 built-in alert rules for automatic anomaly detection
- **Health Checks** - Database status, table integrity, data coverage
- **Performance Monitoring** - Query performance, cache hit rate, system resource usage

## 📊 Database Architecture

SimTradeData uses a carefully designed 11-table architecture supporting multi-market, multi-frequency data:

| Table | Function | Features |
|-------|----------|----------|
| `stocks` | Stock basic info | Code, name, market, industry classification |
| `market_data` | Market quotes | OHLCV, multi-frequency (1d/5m/15m/30m/60m) |
| `valuations` | Valuation metrics | PE, PB, PS, PCF ratios |
| `financials` | Core financial data | 49 core financial indicators |
| `balance_sheet_detail` | Balance sheet | 110+ detailed items (JSON storage) |
| `income_statement_detail` | Income statement | 55+ detailed items (JSON storage) |
| `cash_flow_detail` | Cash flow statement | 75+ detailed items (JSON storage) |
| `trading_calendar` | Trading calendar | Trading days, holidays, suspension info |
| `adjustments` | Adjustment data | Ex-rights, ex-dividend, share capital changes |
| `industry_classification` | Industry classification | Multi-level industry standards |
| `data_source_quality` | Data quality monitoring | Quality scoring, reliability tracking |

For complete architecture design, see [Architecture_Guide.md](docs/Architecture_Guide.md)

## 📊 Technical Comparison

| Feature | Traditional Solution | SimTradeData | Advantage |
|---------|---------------------|--------------|-----------|
| Data Redundancy | 30% | 0% | Completely eliminated |
| Query Performance | Baseline | 2-5x | Significant improvement |
| Data Source Mgmt | Single | Multi-source fusion | High availability |
| Quality Monitoring | None | Real-time | New feature |
| Maintenance Cost | High | Low | Significantly reduced |

## 🏗️ Core Components

- **APIRouter** - High-performance query router with caching and concurrency support
- **SyncManager** - Complete data sync system (incremental update, historical backfill, gap repair)
- **DataSourceManager** - Multi-source manager (BaoStock, Mootdx, QStock)
- **DataQualityMonitor** - Data quality monitor
- **AlertSystem** - Alert system (6 built-in rules)
- **TechnicalIndicators** - Technical indicator calculation engine (vectorized optimization)

## ✅ Project Status

### Core Features (100% Complete)
- ✅ **Data Sync** - Incremental update, historical backfill, gap detection, resume support
- ✅ **Data Query** - Multi-market, multi-frequency, high-performance queries
- ✅ **Data Validation** - Completeness check, quality scoring
- ✅ **Monitoring & Alerts** - Real-time monitoring, automatic alerts

### Test Coverage (100% Complete)
- ✅ **466 Test Cases** - 100% pass rate
- ✅ **Unit Tests** - Complete coverage of core modules
- ✅ **Integration Tests** - End-to-end functionality verification
- ✅ **Sync Tests** - Complete data sync functionality verification

### Documentation (100% Complete)
- ✅ **Architecture Docs** - Complete design guide
- ✅ **Developer Docs** - Detailed developer guide
- ✅ **API Docs** - Complete API reference
- ✅ **Deployment Docs** - Production deployment guide

---

**Project Features**: Designed for Trading Simulation | Zero Technical Debt | Production Ready | 100% Test Pass

**Related Projects**:
- [SimTradeLab](https://github.com/ykayz/SimTradeLab) - Quantitative strategy backtesting framework
- [SimTradeML](https://github.com/ykayz/SimTradeML) - Machine learning model training platform

**Documentation**: [Architecture_Guide.md](docs/Architecture_Guide.md) | [DEPLOYMENT.md](docs/DEPLOYMENT.md)
