"""
数据处理引擎

统一的数据处理引擎，负责股票数据的获取、验证、清洗和存储。
"""

# 标准库导入
import logging
from datetime import date
from typing import Any, Dict, List, Optional

# 项目内导入
from ..config import Config
from ..core.base_manager import BaseManager
from ..core.error_handling import ValidationError, unified_error_handler
from ..data_sources.manager import DataSourceManager
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class DataProcessingEngine(BaseManager):
    """数据处理引擎"""

    def __init__(
        self,
        db_manager: DatabaseManager,
        data_source_manager: DataSourceManager,
        config: Optional[Config] = None,
        **kwargs,
    ):
        """
        初始化数据处理引擎

        Args:
            db_manager: 数据库管理器
            data_source_manager: 数据源管理器
            config: 配置对象
        """
        super().__init__(
            config=config,
            db_manager=db_manager,
            data_source_manager=data_source_manager,
            **kwargs,
        )

    def _init_specific_config(self):
        """初始化数据处理引擎特定配置"""
        self.enable_technical_indicators = self._get_config(
            "enable_technical_indicators", True
        )
        self.enable_valuations = self._get_config("enable_valuations", True)
        self.data_quality_threshold = self._get_config("data_quality_threshold", 0.8)
        self.max_price_change_pct = self._get_config("max_price_change_pct", 20.0)

    def _init_components(self):
        """初始化处理引擎组件"""
        # 验证依赖项存在
        if not hasattr(self, "db_manager") or self.db_manager is None:
            raise ValueError("数据库管理器不能为空")
        if not hasattr(self, "data_source_manager") or self.data_source_manager is None:
            raise ValueError("数据源管理器不能为空")

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return ["db_manager", "data_source_manager"]

    @unified_error_handler(return_dict=True)
    def process_symbol_data(
        self,
        symbol: str,
        start_date: date,
        end_date: Optional[date] = None,
        frequency: str = "1d",
        force_update: bool = False,
    ) -> Dict[str, Any]:
        """处理股票数据"""
        if not symbol:
            raise ValidationError("股票代码不能为空")

        if not start_date:
            raise ValidationError("开始日期不能为空")

        if end_date is None:
            end_date = start_date

        self._log_method_start(
            "process_symbol_data",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

        result = {
            "symbol": symbol,
            "processed_dates": [],
            "skipped_dates": [],
            "failed_dates": [],
            "total_records": 0,
        }

        # 获取原始数据
        raw_data = self.data_source_manager.get_daily_data(symbol, start_date, end_date)

        # 处理数据
        if hasattr(raw_data, "iterrows"):
            for _, row in raw_data.iterrows():
                daily_data = row.to_dict()

                # 验证数据
                validated_data = self._validate_data(daily_data)

                # 存储数据
                self._store_market_data(
                    validated_data, symbol, daily_data.get("date", start_date)
                )

                result["processed_dates"].append(
                    str(daily_data.get("date", start_date))
                )

        result["total_records"] = len(result["processed_dates"])

        self._log_method_end(
            "process_symbol_data", records=result["total_records"], symbol=symbol
        )

        return result

    @unified_error_handler(return_dict=True)
    def process_stock_data(
        self,
        symbol: str,
        start_date: date,
        end_date: Optional[date] = None,
        frequency: str = "1d",
        force_update: bool = False,
    ) -> Dict[str, Any]:
        """处理股票数据 - 别名方法"""
        return self.process_symbol_data(
            symbol, start_date, end_date, frequency, force_update
        )

    def _validate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """数据验证和清洗"""
        # 字段映射
        field_mapping = {
            "open": ["open", "开盘"],
            "high": ["high", "最高"],
            "low": ["low", "最低"],
            "close": ["close", "收盘"],
            "volume": ["volume", "成交量"],
        }

        def safe_float(value, default=0.0):
            """安全的浮点数转换"""
            if value is None or value == "" or str(value).strip() == "":
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        validated_data = {}
        for std_field, possible_fields in field_mapping.items():
            for field in possible_fields:
                if field in data:
                    validated_data[std_field] = safe_float(data[field])
                    break
            else:
                validated_data[std_field] = 0.0

        # 数据质量检查
        if validated_data["high"] < validated_data["low"]:
            self._log_warning("_validate_data", "最高价低于最低价，数据异常")

        if validated_data["close"] <= 0:
            self._log_warning("_validate_data", "收盘价为零或负数，数据异常")

        return validated_data

    def _store_market_data(self, data: Dict[str, Any], symbol: str, trade_date):
        """存储市场数据"""
        sql = """
        INSERT OR REPLACE INTO market_data (
            symbol, date, frequency, open, high, low, close, volume, amount, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = (
            symbol,
            str(trade_date),
            "1d",
            data.get("open", 0),
            data.get("high", 0),
            data.get("low", 0),
            data.get("close", 0),
            data.get("volume", 0),
            data.get("amount", 0),
            "processed",
        )

        self.db_manager.execute(sql, params)

    @unified_error_handler(return_dict=True)
    def process_symbols_batch_pipeline(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        batch_size: int = 10,
        max_workers: int = 4,
        progress_bar=None,
    ) -> Dict[str, Any]:
        """批量处理股票数据"""
        if not symbols:
            raise ValidationError("股票代码列表不能为空")

        if not start_date:
            raise ValidationError("开始日期不能为空")

        if not end_date:
            raise ValidationError("结束日期不能为空")

        self._log_method_start(
            "process_symbols_batch_pipeline",
            symbols_count=len(symbols),
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        )

        result = {
            "success_count": 0,
            "error_count": 0,
            "total_records": 0,
            "processed_symbols": [],
            "failed_symbols": [],
        }

        # 批量处理
        for symbol in symbols:
            try:
                symbol_result = self.process_symbol_data(symbol, start_date, end_date)

                # 检查是否成功（统一错误处理返回字典格式）
                if isinstance(symbol_result, dict) and symbol_result.get(
                    "success", True
                ):
                    data = symbol_result.get("data", symbol_result)
                    if data["total_records"] > 0:
                        result["success_count"] += 1
                        result["processed_symbols"].append(symbol)
                        result["total_records"] += data["total_records"]
                    else:
                        result["error_count"] += 1
                        result["failed_symbols"].append(symbol)
                else:
                    result["error_count"] += 1
                    result["failed_symbols"].append(symbol)

            except Exception as e:
                self._log_error("process_symbols_batch_pipeline", e, symbol=symbol)
                result["error_count"] += 1
                result["failed_symbols"].append(symbol)

            if progress_bar:
                progress_bar.update(1)

        self._log_method_end(
            "process_symbols_batch_pipeline",
            success_count=result["success_count"],
            error_count=result["error_count"],
            total_records=result["total_records"],
        )

        return result

    def _get_market_from_symbol(self, symbol: str) -> str:
        """从股票代码获取市场"""
        if symbol.endswith(".SZ"):
            return "SZ"
        elif symbol.endswith(".SS"):
            return "SS"
        elif symbol.endswith(".HK"):
            return "HK"
        elif symbol.endswith(".US"):
            return "US"
        else:
            return "CN"

    def _calculate_quality_score(self, result: Dict[str, Any]) -> float:
        """计算质量分数"""
        total = len(result["processed_dates"]) + len(result["failed_dates"])
        if total == 0:
            return 0.0
        return len(result["processed_dates"]) / total * 100
