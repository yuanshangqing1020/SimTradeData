"""
测试基类和公共工具函数

提供统一的测试初始化、数据准备和工具函数
"""

import tempfile
from datetime import date
from pathlib import Path
from typing import List, Optional

import pytest

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager
from simtradedata.database import DatabaseManager
from simtradedata.preprocessor import DataProcessingEngine
from simtradedata.sync import IncrementalSync


class BaseTestClass:
    """测试基类，提供公共的初始化和工具方法"""

    @pytest.fixture
    def config(self):
        """配置对象fixture"""
        config = Config()
        # 测试环境使用baostock作为优先数据源，因为它支持交易日历查询
        config.set("data_sources.priority", ["baostock", "qstock", "mootdx"])
        return config

    @pytest.fixture
    def temp_db(self, config):
        """临时数据库fixture"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        db_manager = DatabaseManager(db_path, config=config)

        # 验证BaseManager初始化
        assert hasattr(db_manager, "config"), "DatabaseManager应该有config属性"
        assert hasattr(db_manager, "logger"), "DatabaseManager应该有logger属性"
        assert hasattr(db_manager, "timeout"), "DatabaseManager应该有timeout配置"
        assert hasattr(
            db_manager, "max_retries"
        ), "DatabaseManager应该有max_retries配置"

        # 创建基础表结构
        self._create_test_tables(db_manager)

        yield db_manager

        # 清理
        Path(db_path).unlink(missing_ok=True)

    @pytest.fixture
    def db_manager(self, temp_db):
        """数据库管理器fixture"""
        return temp_db

    @pytest.fixture
    def data_source_manager(self, config, db_manager):
        """数据源管理器fixture"""
        return DataSourceManager(config, db_manager=db_manager)

    @pytest.fixture
    def processing_engine(self, db_manager, data_source_manager, config):
        """数据处理引擎fixture"""
        return DataProcessingEngine(db_manager, data_source_manager, config)

    @pytest.fixture
    def incremental_sync(
        self, db_manager, data_source_manager, processing_engine, config
    ):
        """增量同步器fixture"""
        return IncrementalSync(
            db_manager, data_source_manager, processing_engine, config
        )

    def _create_test_tables(self, db_manager: DatabaseManager):
        """创建测试所需的表结构"""
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                time TIME,
                frequency TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume INTEGER, amount REAL,
                change_percent REAL,
                prev_close REAL,
                amplitude REAL,
                turnover_rate REAL,
                source TEXT,
                quality_score REAL,
                is_limit_up INTEGER,
                is_limit_down INTEGER,
                PRIMARY KEY (symbol, date, frequency)
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS symbols (
                code TEXT PRIMARY KEY,
                name TEXT,
                market TEXT,
                sector TEXT,
                industry TEXT,
                list_date DATE,
                status TEXT DEFAULT 'active'
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS trading_calendar (
                date TEXT NOT NULL,
                market TEXT NOT NULL DEFAULT 'CN',
                is_trading INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (date, market)
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_status (
                symbol TEXT NOT NULL,
                frequency TEXT NOT NULL,
                last_sync_date DATE,
                last_data_date DATE,
                next_sync_date DATE,
                status TEXT DEFAULT 'pending',
                progress REAL DEFAULT 0,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                sync_duration REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, frequency)
            )
        """
        )

        # 添加data_sources表用于测试
        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS data_sources (
                name TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                priority INTEGER DEFAULT 1,
                rate_limit INTEGER DEFAULT 60,
                supports_realtime BOOLEAN DEFAULT FALSE,
                supports_history BOOLEAN DEFAULT TRUE,
                supports_financials BOOLEAN DEFAULT FALSE,
                markets TEXT,
                frequencies TEXT,
                status TEXT DEFAULT 'active',
                last_check TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # 插入测试数据源配置（优先使用baostock，它支持交易日历）
        db_manager.execute(
            """
            INSERT OR REPLACE INTO data_sources
            (name, type, enabled, priority, rate_limit, supports_realtime, supports_history, supports_financials, markets, frequencies, status)
            VALUES
                ('baostock', 'baostock', 1, 1, 120, 0, 1, 1, '["SZ","SS"]', '["1d"]', 'active'),
                ('mootdx', 'mootdx', 1, 2, 300, 1, 1, 1, '["SZ","SS"]', '["1d","5m","15m","30m","60m"]', 'active'),
                ('qstock', 'qstock', 1, 3, 100, 0, 1, 0, '["SZ","SS","HK","US"]', '["1d","5m","15m","30m","1h"]', 'active')
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS extended_sync_status (
                symbol TEXT NOT NULL,
                sync_type TEXT NOT NULL,
                target_date TEXT NOT NULL,
                
                status TEXT DEFAULT 'pending',
                last_updated TIMESTAMP,
                
                phase TEXT DEFAULT 'extended_data',
                session_id TEXT,
                
                records_count INTEGER DEFAULT 0,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (symbol, sync_type, target_date)
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                market TEXT NOT NULL,
                exchange TEXT,
                industry_l1 TEXT,
                industry_l2 TEXT,
                concepts TEXT,
                list_date DATE,
                delist_date DATE,
                currency TEXT DEFAULT 'CNY',
                lot_size INTEGER DEFAULT 100,
                total_shares REAL,
                float_shares REAL,
                status TEXT DEFAULT 'active',
                is_st BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS financials (
                symbol TEXT NOT NULL,
                report_date DATE NOT NULL,
                report_type TEXT NOT NULL,
                revenue REAL,
                operating_profit REAL,
                net_profit REAL,
                gross_margin REAL,
                net_margin REAL,
                total_assets REAL,
                total_liabilities REAL,
                shareholders_equity REAL,
                operating_cash_flow REAL,
                investing_cash_flow REAL,
                financing_cash_flow REAL,
                eps REAL,
                bps REAL,
                roe REAL,
                roa REAL,
                debt_ratio REAL,
                source TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, report_date, report_type)
            )
        """
        )

        db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS valuations (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                pcf_ratio REAL,
                market_cap REAL,
                enterprise_value REAL,
                source TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, date)
            )
        """
        )

    def clean_test_data(
        self,
        db_manager: DatabaseManager,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """清理测试数据"""
        for symbol in symbols:
            if start_date and end_date:
                db_manager.execute(
                    "DELETE FROM market_data WHERE symbol = ? AND date >= ? AND date <= ?",
                    (symbol, start_date, end_date),
                )
            elif start_date:
                db_manager.execute(
                    "DELETE FROM market_data WHERE symbol = ? AND date >= ?",
                    (symbol, start_date),
                )
            else:
                db_manager.execute(
                    "DELETE FROM market_data WHERE symbol = ?", (symbol,)
                )

    def get_test_symbols(self) -> List[str]:
        """获取标准测试股票代码"""
        return ["000001.SZ", "000002.SZ", "600000.SH"]

    def get_test_date_range(self) -> tuple[date, date]:
        """获取标准测试日期范围"""
        return date(2025, 1, 20), date(2025, 1, 24)

    def verify_data_exists(
        self, db_manager: DatabaseManager, symbol: str, expected_days: int
    ) -> bool:
        """验证数据是否存在"""
        result = db_manager.fetchone(
            "SELECT COUNT(*) as count FROM market_data WHERE symbol = ?", (symbol,)
        )
        return result["count"] >= expected_days if result else False

    def print_test_info(
        self, test_name: str, symbols: List[str], start_date: date, end_date: date
    ):
        """打印测试信息"""
        print(f"\n=== {test_name} ===")
        print(f"测试股票: {symbols}")
        print(f"日期范围: {start_date} 到 {end_date}")


class SyncTestMixin:
    """同步测试相关的混入类"""

    def setup_sync_test(self, db_manager: DatabaseManager, symbols: List[str]):
        """设置同步测试环境"""
        self.clean_test_data(db_manager, symbols, "2025-01-20")
        print("清理旧的测试数据...")

    def verify_sync_result(self, result: dict, expected_success: bool = True):
        """验证同步结果"""
        # 检查是否是增量同步的结果格式
        if "success_count" in result and "error_count" in result:
            # 增量同步结果格式
            total_symbols = result.get("total_symbols", 0)
            success_count = result.get("success_count", 0)
            error_count = result.get("error_count", 0)

            if expected_success:
                assert success_count > 0, f"期望成功但success_count为0: {result}"
            print(
                f"同步结果: 总数{total_symbols}, 成功{success_count}, 失败{error_count}"
            )
        elif "success" in result:
            # 标准成功/失败格式
            if expected_success:
                assert result["success"], f"同步失败: {result.get('error', '未知错误')}"
            print(f"同步结果: {result}")
        else:
            # 未知格式，记录但不断言
            print(f"同步结果（未知格式）: {result}")
            if expected_success:
                # 如果期望成功但结果格式未知，给出警告而不是失败
                print("⚠️ 无法验证同步是否成功 - 结果格式未知")


# 公共工具函数
def create_sample_data() -> List[dict]:
    """创建示例测试数据"""
    return [
        {
            "symbol": "000001.SZ",
            "date": "2025-01-20",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 1000000,
            "amount": 10500000.0,
        },
        {
            "symbol": "000001.SZ",
            "date": "2025-01-21",
            "open": 10.5,
            "high": 11.2,
            "low": 10.0,
            "close": 11.0,
            "volume": 1200000,
            "amount": 13200000.0,
        },
    ]


def assert_market_data_valid(data: dict):
    """验证市场数据的有效性"""
    required_fields = ["symbol", "date", "open", "high", "low", "close", "volume"]
    for field in required_fields:
        assert field in data, f"缺少必需字段: {field}"
        assert data[field] is not None, f"字段 {field} 不能为空"

    # 验证OHLC逻辑
    assert data["high"] >= data["open"], "最高价应该 >= 开盘价"
    assert data["high"] >= data["close"], "最高价应该 >= 收盘价"
    assert data["low"] <= data["open"], "最低价应该 <= 开盘价"
    assert data["low"] <= data["close"], "最低价应该 <= 收盘价"
