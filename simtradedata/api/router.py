"""
API路由器核心

提供统一的数据查询接口，支持缓存、格式化和错误处理。
"""

# 标准库导入
import logging
import time
from datetime import date
from typing import Any, Dict, List, Union

# 项目内导入
from ..config import Config
from ..database import DatabaseManager
from .cache import QueryCache
from .formatters import ResultFormatter
from .query_builders import (
    FundamentalsQueryBuilder,
    HistoryQueryBuilder,
    SnapshotQueryBuilder,
    StockInfoQueryBuilder,
)

logger = logging.getLogger(__name__)


class APIRouter:
    """API路由器"""

    def __init__(self, db_manager: DatabaseManager, config: Config = None):
        """
        初始化API路由器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
        """
        self.db_manager = db_manager
        self.config = config or Config()

        # 初始化组件
        self.history_builder = HistoryQueryBuilder(config)
        self.snapshot_builder = SnapshotQueryBuilder(config)
        self.fundamentals_builder = FundamentalsQueryBuilder(config)
        self.stock_info_builder = StockInfoQueryBuilder(config)
        self.formatter = ResultFormatter(config)
        self.cache = QueryCache(config)

        # API配置
        self.enable_cache = self.config.get("api.cache_enabled", True)
        self.enable_query_log = self.config.get("api.query_log_enabled", True)
        self.query_timeout = self.config.get("api.query_timeout", 30)

        logger.info("API router initialized")

    def get_history(
        self,
        symbols: Union[str, List[str]],
        start_date: Union[str, date],
        end_date: Union[str, date] = None,
        frequency: str = "1d",
        fields: List[str] = None,
        format_type: str = None,
        limit: int = None,
        offset: int = 0,
        use_cache: bool = True,
    ) -> Any:
        """
        获取历史数据

        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率
            fields: 查询字段列表
            format_type: 输出格式
            limit: 限制数量
            offset: 偏移量
            use_cache: 是否使用缓存

        Returns:
            Any: 查询结果
        """
        try:
            start_time = time.time()

            # 参数标准化
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols = self.history_builder.normalize_symbols(symbols)
            start_date_str, end_date_str = self.history_builder.parse_date_range(
                start_date, end_date
            )
            frequency = self.history_builder.validate_frequency(frequency)

            # 生成缓存键
            cache_key = None
            if self.enable_cache and use_cache:
                cache_key = self.cache.generate_cache_key(
                    "history",
                    symbols=symbols,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    frequency=frequency,
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

                # 尝试从缓存获取
                cached_result = self.cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"historical data cache hit : {cache_key}")
                    return cached_result

            # 构建查询
            sql, params = self.history_builder.build_query(
                symbols=symbols,
                start_date=start_date_str,
                end_date=end_date_str,
                frequency=frequency,
                fields=fields,
                limit=limit,
                offset=offset,
            )

            # 执行查询
            raw_data = self._execute_query(sql, params, "get_history")

            # 格式化结果
            result = self.formatter.format_history_result(
                raw_data, symbols, start_date_str, end_date_str, frequency, format_type
            )

            # 缓存结果
            if self.enable_cache and use_cache and cache_key:
                self.cache.set(cache_key, result)

            # 记录查询日志
            if self.enable_query_log:
                query_time = time.time() - start_time
                self._log_query("get_history", len(raw_data), query_time, symbols)

            return result

        except Exception as e:
            logger.error(f"failed to retrieve historical data : {e}")
            return self.formatter.format_error_result(
                str(e), "HISTORY_QUERY_ERROR", format_type
            )

    def get_snapshot(
        self,
        symbols: Union[str, List[str]] = None,
        trade_date: Union[str, date] = None,
        market: str = None,
        fields: List[str] = None,
        format_type: str = None,
        limit: int = None,
        offset: int = 0,
        use_cache: bool = True,
    ) -> Any:
        """
        获取快照数据

        Args:
            symbols: 股票代码列表，为None时查询所有
            trade_date: 交易日期，为None时查询最新
            market: 市场过滤
            fields: 查询字段列表
            format_type: 输出格式
            limit: 限制数量
            offset: 偏移量
            use_cache: 是否使用缓存

        Returns:
            Any: 查询结果
        """
        try:
            start_time = time.time()

            # 参数标准化
            if symbols:
                symbols = self.snapshot_builder.normalize_symbols(symbols)

            trade_date_str = None
            if trade_date:
                if isinstance(trade_date, str):
                    trade_date_str = trade_date
                else:
                    trade_date_str = trade_date.strftime("%Y-%m-%d")

            if market:
                market = self.snapshot_builder.validate_market(market)

            # 生成缓存键
            cache_key = None
            if self.enable_cache and use_cache:
                cache_key = self.cache.generate_cache_key(
                    "snapshot",
                    symbols=symbols,
                    trade_date=trade_date_str,
                    market=market,
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

                # 尝试从缓存获取
                cached_result = self.cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"snapshot data cache hit : {cache_key}")
                    return cached_result

            # 构建查询
            sql, params = self.snapshot_builder.build_query(
                symbols=symbols,
                trade_date=trade_date_str,
                market=market,
                fields=fields,
                limit=limit,
                offset=offset,
            )

            # 执行查询
            raw_data = self._execute_query(sql, params, "get_snapshot")

            # 格式化结果
            result = self.formatter.format_snapshot_result(
                raw_data, trade_date_str, market, format_type
            )

            # 缓存结果
            if self.enable_cache and use_cache and cache_key:
                self.cache.set(cache_key, result)

            # 记录查询日志
            if self.enable_query_log:
                query_time = time.time() - start_time
                self._log_query("get_snapshot", len(raw_data), query_time, symbols)

            return result

        except Exception as e:
            logger.error(f"failed to retrieve snapshot data : {e}")
            return self.formatter.format_error_result(
                str(e), "SNAPSHOT_QUERY_ERROR", format_type
            )

    def get_fundamentals(
        self,
        symbols: Union[str, List[str]],
        report_date: Union[str, date] = None,
        report_type: str = None,
        fields: List[str] = None,
        format_type: str = None,
        limit: int = None,
        offset: int = 0,
        use_cache: bool = True,
    ) -> Any:
        """
        获取财务数据

        Args:
            symbols: 股票代码列表
            report_date: 报告期
            report_type: 报告类型
            fields: 查询字段列表
            format_type: 输出格式
            limit: 限制数量
            offset: 偏移量
            use_cache: 是否使用缓存

        Returns:
            Any: 查询结果
        """
        try:
            start_time = time.time()

            # 参数标准化
            symbols = self.fundamentals_builder.normalize_symbols(symbols)

            report_date_str = None
            if report_date:
                if isinstance(report_date, str):
                    report_date_str = report_date
                else:
                    report_date_str = report_date.strftime("%Y-%m-%d")

            # 生成缓存键
            cache_key = None
            if self.enable_cache and use_cache:
                cache_key = self.cache.generate_cache_key(
                    "fundamentals",
                    symbols=symbols,
                    report_date=report_date_str,
                    report_type=report_type,
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

                # 尝试从缓存获取
                cached_result = self.cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"financial data cache hit : {cache_key}")
                    return cached_result

            # 构建查询
            sql, params = self.fundamentals_builder.build_query(
                symbols=symbols,
                report_date=report_date_str,
                report_type=report_type,
                fields=fields,
                limit=limit,
                offset=offset,
            )

            # 执行查询
            raw_data = self._execute_query(sql, params, "get_fundamentals")

            # 格式化结果
            result = self.formatter.format_fundamentals_result(
                raw_data, symbols, report_date_str, report_type, format_type
            )

            # 缓存结果
            if self.enable_cache and use_cache and cache_key:
                self.cache.set(cache_key, result)

            # 记录查询日志
            if self.enable_query_log:
                query_time = time.time() - start_time
                self._log_query("get_fundamentals", len(raw_data), query_time, symbols)

            return result

        except Exception as e:
            logger.error(f"failed to retrieve financial data : {e}")
            return self.formatter.format_error_result(
                str(e), "FUNDAMENTALS_QUERY_ERROR", format_type
            )

    def get_stock_info(
        self,
        symbols: Union[str, List[str]] = None,
        market: str = None,
        industry: str = None,
        status: str = "active",
        fields: List[str] = None,
        format_type: str = None,
        limit: int = None,
        offset: int = 0,
        use_cache: bool = True,
    ) -> Any:
        """
        获取股票信息

        Args:
            symbols: 股票代码列表，为None时查询所有
            market: 市场过滤
            industry: 行业过滤
            status: 状态过滤
            fields: 查询字段列表
            format_type: 输出格式
            limit: 限制数量
            offset: 偏移量
            use_cache: 是否使用缓存

        Returns:
            Any: 查询结果
        """
        try:
            start_time = time.time()

            # 参数标准化
            if symbols:
                symbols = self.stock_info_builder.normalize_symbols(symbols)

            if market:
                market = self.stock_info_builder.validate_market(market)

            # 生成缓存键
            cache_key = None
            if self.enable_cache and use_cache:
                cache_key = self.cache.generate_cache_key(
                    "stock_info",
                    symbols=symbols,
                    market=market,
                    industry=industry,
                    status=status,
                    fields=fields,
                    limit=limit,
                    offset=offset,
                )

                # 尝试从缓存获取
                cached_result = self.cache.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"stock info cache hit : {cache_key}")
                    return cached_result

            # 构建查询
            sql, params = self.stock_info_builder.build_query(
                symbols=symbols,
                market=market,
                industry=industry,
                status=status,
                fields=fields,
                limit=limit,
                offset=offset,
            )

            # 执行查询
            raw_data = self._execute_query(sql, params, "get_stock_info")

            # 格式化结果
            result = self.formatter.format_stock_info_result(
                raw_data, market, industry, format_type
            )

            # 缓存结果
            if self.enable_cache and use_cache and cache_key:
                self.cache.set(cache_key, result)

            # 记录查询日志
            if self.enable_query_log:
                query_time = time.time() - start_time
                self._log_query("get_stock_info", len(raw_data), query_time, symbols)

            return result

        except Exception as e:
            logger.error(f"failed to retrieve stock info : {e}")
            return self.formatter.format_error_result(
                str(e), "STOCK_INFO_QUERY_ERROR", format_type
            )

    def query(self, params: Dict[str, Any]) -> Any:
        """通用查询接口，用于保持对旧调用方式的兼容"""
        if not isinstance(params, dict):
            raise ValueError("params must be a dictionary")

        data_type = params.get("data_type")
        format_type = params.get("format")

        if not data_type:
            raise ValueError("data_type is required in query parameters")

        def _normalize_sequence(value):
            if value is None:
                return None
            if isinstance(value, str):
                parts = [item.strip() for item in value.split(",") if item.strip()]
                return parts if len(parts) > 1 else parts[0] if parts else None
            return value

        def _normalize_fields(value):
            normalized = _normalize_sequence(value)
            if normalized is None:
                return None
            if isinstance(normalized, list):
                return normalized
            return [normalized]

        data_type = str(data_type).lower()

        try:
            if data_type in {"stock_list", "stock_info"}:
                symbols = _normalize_sequence(params.get("symbols"))
                result = self.get_stock_info(
                    symbols=symbols,
                    market=params.get("market"),
                    industry=params.get("industry"),
                    status=params.get("status", "active"),
                    fields=_normalize_fields(params.get("fields")),
                    format_type=format_type,
                    limit=params.get("limit"),
                    offset=params.get("offset", 0),
                    use_cache=params.get("use_cache", True),
                )
            elif data_type in {"price_data", "history"}:
                symbols = _normalize_sequence(params.get("symbols"))
                result = self.get_history(
                    symbols=symbols,
                    start_date=params.get("start_date"),
                    end_date=params.get("end_date"),
                    frequency=params.get("frequency", "1d"),
                    fields=_normalize_fields(params.get("fields")),
                    format_type=format_type,
                    limit=params.get("limit"),
                    offset=params.get("offset", 0),
                    use_cache=params.get("use_cache", True),
                )
            elif data_type in {"fundamentals", "financials"}:
                symbols = _normalize_sequence(params.get("symbols"))
                result = self.get_fundamentals(
                    symbols=symbols,
                    report_date=params.get("report_date"),
                    report_type=params.get("report_type"),
                    fields=_normalize_fields(params.get("fields")),
                    format_type=format_type,
                    limit=params.get("limit"),
                    offset=params.get("offset", 0),
                    use_cache=params.get("use_cache", True),
                )
            elif data_type in {"snapshot", "quote"}:
                symbols = _normalize_sequence(params.get("symbols"))
                result = self.get_snapshot(
                    symbols=symbols,
                    trade_date=params.get("trade_date"),
                    market=params.get("market"),
                    fields=_normalize_fields(params.get("fields")),
                    format_type=format_type,
                    limit=params.get("limit"),
                    offset=params.get("offset", 0),
                    use_cache=params.get("use_cache", True),
                )
            elif data_type == "api_stats":
                result = self.get_api_stats()
            elif data_type == "clear_cache":
                result = self.clear_cache(params.get("pattern"))
            else:
                raise ValueError(f"Unsupported data_type: {data_type}")

            return result

        except Exception as exc:
            logger.error(f"failed to execute query: {exc}")
            raise

    def _execute_query(
        self, sql: str, params: List[Any], query_type: str
    ) -> List[Dict[str, Any]]:
        """
        执行SQL查询

        Args:
            sql: SQL语句
            params: 查询参数
            query_type: 查询类型

        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        try:
            start_time = time.time()

            # 执行查询
            raw_results = self.db_manager.fetchall(sql, params)

            # 将sqlite3.Row对象转换为字典
            results = [dict(row) for row in raw_results]

            query_time = time.time() - start_time
            logger.debug(
                f"SQL query completed : {query_type}, elapsed : {query_time:.3f}s, result count : {len(results)}"
            )

            return results

        except Exception as e:
            logger.error(f"SQL query failed {query_type}: {e}")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"parameters : {params}")
            raise

    def _log_query(
        self,
        query_type: str,
        result_count: int,
        query_time: float,
        symbols: List[str] = None,
    ):
        """记录查询日志"""
        try:
            symbol_count = len(symbols) if symbols else 0
            logger.info(
                f"query completed : {query_type}, stock count : {symbol_count},"
                f"结果数: {result_count}, 耗时: {query_time:.3f}s"
            )
        except Exception as e:
            logger.error(f"records query log failed : {e}")

    def get_api_stats(self) -> Dict[str, Any]:
        """获取API统计信息"""
        try:
            cache_stats = self.cache.get_cache_stats()
            formatter_info = self.formatter.get_format_info()

            return {
                "cache": cache_stats,
                "formatter": formatter_info,
                "config": {
                    "enable_cache": self.enable_cache,
                    "enable_query_log": self.enable_query_log,
                    "query_timeout": self.query_timeout,
                },
                "builders": {
                    "history": {
                        "max_symbols_per_query": self.history_builder.max_symbols_per_query,
                        "max_date_range_days": self.history_builder.max_date_range_days,
                        "supported_frequencies": self.history_builder.supported_frequencies,
                    },
                    "supported_markets": self.history_builder.supported_markets,
                },
            }
        except Exception as e:
            logger.error(f"failed to retrieve API statistics : {e}")
            return {"error": str(e)}

    def clear_cache(self, pattern: str = None) -> Dict[str, Any]:
        """
        清理缓存

        Args:
            pattern: 匹配模式，为None时清空所有缓存

        Returns:
            Dict[str, Any]: 清理结果
        """
        try:
            if pattern:
                cleared_count = self.cache.invalidate_pattern(pattern)
                return {"cleared_count": cleared_count, "pattern": pattern}
            else:
                success = self.cache.clear()
                return {"success": success, "action": "clear_all"}
        except Exception as e:
            logger.error(f"failed to clean cache : {e}")
            return {"error": str(e)}
