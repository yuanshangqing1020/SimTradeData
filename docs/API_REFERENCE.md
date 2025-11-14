# SimTradeData API Reference

**[English](API_REFERENCE.md)** | **[中文](API_REFERENCE_CN.md)**

## 📖 Overview

SimTradeData provides multiple API interfaces including PTrade-compatible interface, REST API, and Python API. This document details all available API interfaces and usage methods.

## 🐍 Python API

### Core API Router

#### APIRouter

High-performance data query router providing unified data access interface with caching, concurrency, and query optimization support.

```python
from simtradedata.api.router import APIRouter
from simtradedata.database.manager import DatabaseManager
from simtradedata.config.manager import Config

# Initialize core components
config = Config()
db_manager = DatabaseManager("data/simtradedata.db")
api_router = APIRouter(db_manager, config)
```

#### Core Features

- **High-Performance Queries**: Optimized SQL generation and execution
- **Intelligent Caching**: Multi-level cache strategy for improved query speed
- **Concurrency Support**: Support for high-concurrency query requests
- **Formatted Output**: Automatic formatting to DataFrame or JSON
- **Error Handling**: Comprehensive exception handling and logging

### Main API Methods

#### Historical Data Query

**get_history(symbols, start_date, end_date, frequency="1d", fields=None)**
- Get historical market data, support multi-symbol and multi-frequency queries
- Parameters:
  - `symbols` (list[str]): Stock code list, e.g., ['000001.SZ', '000002.SZ']
  - `start_date` (str): Start date, format 'YYYY-MM-DD'
  - `end_date` (str): End date, format 'YYYY-MM-DD'
  - `frequency` (str): Data frequency, supports '1d', '5m', '15m', '30m', '60m'
  - `fields` (list[str], optional): Specify return fields
- Returns: pandas.DataFrame

```python
# Get single stock daily data
data = api_router.get_history(
    symbols=['000001.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    frequency='1d'
)

# Get multiple stocks minute data
data = api_router.get_history(
    symbols=['000001.SZ', '000002.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    frequency='5m'
)
```

#### Real-time Data Query

**get_snapshot(symbols, fields=None)**
- Get stock snapshot data
- Parameters:
  - `symbols` (list[str]): Stock code list
  - `fields` (list[str], optional): Specify return fields
- Returns: pandas.DataFrame

```python
# Get stock snapshot
snapshot = api_router.get_snapshot(['000001.SZ', '000002.SZ'])
```

#### Financial Data Query

**get_financials(symbols, start_date, end_date)**
- Get financial data
- Parameters:
  - `symbols` (list[str]): Stock code list
  - `start_date` (str): Start date
  - `end_date` (str): End date
- Returns: pandas.DataFrame

```python
# Get financial data
financials = api_router.get_financials(
    symbols=['000001.SZ'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)
```

### Data Sync API

```python
from simtradedata.sync import SyncManager

# Initialize sync manager
sync_manager = SyncManager(db_manager, data_source_manager)

# Incremental sync
result = sync_manager.incremental_sync(
    symbol='000001.SZ',
    start_date='2024-01-01',
    end_date='2024-01-31'
)

# Historical backfill
result = sync_manager.historical_backfill(
    symbol='000001.SZ',
    target_date='2024-01-01'
)
```

### Monitoring API

```python
from simtradedata.monitoring import AlertSystem, DataQualityMonitor

# Data quality monitoring
quality_monitor = DataQualityMonitor(db_manager)
quality_score = quality_monitor.evaluate_source_quality('baostock', '000001.SZ', 'ohlcv')

# Alert system
alert_system = AlertSystem(db_manager)
alerts = alert_system.check_all_rules()
summary = alert_system.get_alert_summary()
```

## 🔌 PTrade Compatible Interface

SimTradeData provides PTrade-compatible API interface. For details, see [PTrade API Reference](PTrade_API_mini_Reference.md).

### Usage Example

```python
from simtradedata.interfaces import PTradeAPIAdapter

adapter = PTradeAPIAdapter(db_manager, config)

# Get stock list
stocks = adapter.get_stock_list('SZ')

# Get price data
prices = adapter.get_price('000001.SZ', '2024-01-01', '2024-01-31')

# Get stock info
info = adapter.get_stock_info('000001.SZ')
```

## 🌐 REST API

> **Note**: REST API server functionality is optional and requires separate startup. SimTradeData primarily provides Python API.

### Basic Info

- **Base URL**: `http://localhost:8080/api/v1`
- **Content-Type**: `application/json`

### Main Endpoints

SimTradeData ships with a FastAPI-powered REST surface. Core routes include:

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/v1/health` | Health probe |
| GET | `/api/v1/stocks` | List stocks with optional `market`, `industry`, `status`, `fields`, `limit`, `offset` query params |
| GET | `/api/v1/stocks/{symbol}` | Retrieve a single symbol's profile |
| GET | `/api/v1/stocks/{symbol}/history` | Historical prices (`start_date`, `end_date`, `frequency`, `fields`, `limit`, `offset`) |
| GET | `/api/v1/stocks/{symbol}/fundamentals` | Financial metrics (`report_date`, `report_type`, `fields`) |
| GET | `/api/v1/stocks/{symbol}/snapshot` | Latest snapshot for a symbol |
| GET | `/api/v1/snapshots` | Batch snapshot retrieval for multiple symbols |
| GET | `/api/v1/meta/stats` | Router and cache statistics |

> Legacy route `GET /api/v1/stocks/{symbol}/price` remains available as an alias of the `/history` endpoint.

FastAPI automatically serves interactive docs at `/docs` (Swagger UI) and `/redoc`.

## 📊 Data Source Management API

```python
from simtradedata.data_sources import DataSourceManager

# Initialize data source manager
ds_manager = DataSourceManager(config)

# Health check
health = ds_manager.health_check()

# Get available data sources
available = ds_manager.get_available_sources()

# Get system status
status = ds_manager.get_status()
```

## 📊 Performance API

### Cache Management

```python
from simtradedata.performance import CacheManager

cache = CacheManager(config)

# Set cache
cache.set('key', data, ttl=600)

# Get cache
data = cache.get('key')

# Get cache statistics
stats = cache.get_stats()
print(f"Hit rate: {stats['hit_rate']}%")
```

### Technical Indicators Calculation

```python
from simtradedata.preprocessor.indicators import TechnicalIndicators

indicators = TechnicalIndicators()

# Calculate MACD
macd = indicators.calculate_macd(close_prices)

# Calculate RSI
rsi = indicators.calculate_rsi(close_prices)

# Get cache statistics
stats = indicators.get_cache_stats()
```

## 📈 Monitoring & Health Check

### Database Health Check

```python
from simtradedata.database import DatabaseManager
from simtradedata.config import Config

config = Config()
db = DatabaseManager(config.get('database.path'))

# Check database connection
try:
    result = db.fetchone("SELECT 1")
    print("✅ Database connection OK")
except Exception as e:
    print(f"❌ Database connection failed: {e}")

# Check table status
tables = ['stocks', 'market_data', 'trading_calendar']
for table in tables:
    count = db.fetchone(f"SELECT COUNT(*) as count FROM {table}")
    print(f"✅ {table}: {count['count']} records")
```

### Data Quality Monitoring

```python
from simtradedata.monitoring import DataQualityMonitor

monitor = DataQualityMonitor(db_manager)

# Evaluate data source quality
quality = monitor.evaluate_source_quality('baostock', '000001.SZ', 'ohlcv')
print(f"Quality score: {quality['overall_score']}")

# Get data source ranking
ranking = monitor.get_source_ranking('ohlcv')
```

### Alert System

```python
from simtradedata.monitoring import AlertSystem, AlertRuleFactory, ConsoleNotifier

# Initialize alert system
alert_system = AlertSystem(db_manager)
alert_system.add_notifier(ConsoleNotifier())

# Add default alert rules
rules = AlertRuleFactory.create_all_default_rules(db_manager)
for rule in rules:
    alert_system.add_rule(rule)

# Check alerts
alerts = alert_system.check_all_rules()

# Get alert summary
summary = alert_system.get_alert_summary()
print(f"Active alerts: {summary['active_alerts_count']}")
```

---

*SimTradeData API Reference - Complete API Documentation*
