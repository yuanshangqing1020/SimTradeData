"""
查询优化器

提供SQL查询优化、索引策略优化和查询计划分析功能。
"""

import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from ..config import Config
from ..core import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class QueryOptimizer(BaseManager):
    """查询优化器"""

    def __init__(self, db_manager: DatabaseManager, config: Config = None, **kwargs):
        """
        初始化查询优化器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
        """
        # 设置数据库管理器依赖
        self.db_manager = db_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")

        # 调用BaseManager初始化
        super().__init__(config=config, db_manager=db_manager, **kwargs)

    def _init_specific_config(self):
        """初始化查询优化器特定配置"""
        # 优化配置
        self.enable_query_cache = self._get_config("enable_query_cache", True)
        self.cache_ttl = self._get_config("cache_ttl", 300)  # 5分钟
        self.max_cache_size = self._get_config("max_cache_size", 1000)
        self.enable_explain = self._get_config("enable_explain", True)
        self.slow_query_threshold = self._get_config("slow_query_threshold", 1.0)

    def _init_components(self):
        """初始化查询优化器组件"""
        # 查询缓存
        self.query_cache = {}
        self.cache_stats = {"hits": 0, "misses": 0, "evictions": 0}

        # 查询统计
        self.query_stats = defaultdict(list)

        # 索引建议
        self.index_suggestions = []

        # 查询模式
        self.query_patterns = {
            "date_range": re.compile(
                r"trade_date\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'", re.IGNORECASE
            ),
            "symbol_filter": re.compile(r"symbol\s*=\s*'([^']+)'", re.IGNORECASE),
            "market_filter": re.compile(r"market\s*=\s*'([^']+)'", re.IGNORECASE),
            "order_by": re.compile(r"ORDER\s+BY\s+([^\s]+)", re.IGNORECASE),
            "limit": re.compile(r"LIMIT\s+(\d+)", re.IGNORECASE),
        }

        self.logger.info("查询优化器初始化完成")

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return [
            "db_manager",
            "query_cache",
            "cache_stats",
            "query_stats",
            "query_patterns",
        ]

    def optimize_query(self, sql: str, params: Tuple = None) -> Tuple[str, Tuple]:
        """
        优化SQL查询

        Args:
            sql: 原始SQL语句
            params: 查询参数

        Returns:
            Tuple[str, Tuple]: 优化后的SQL和参数
        """
        try:
            # 1. 分析查询模式
            query_info = self._analyze_query_pattern(sql)

            # 2. 应用优化规则
            optimized_sql = self._apply_optimization_rules(sql, query_info)

            # 3. 参数优化
            optimized_params = self._optimize_parameters(params, query_info)

            # 4. 记录优化信息
            if optimized_sql != sql:
                logger.debug(f"查询已优化: {sql[:100]}... -> {optimized_sql[:100]}...")

            return optimized_sql, optimized_params

        except Exception as e:
            logger.error(f"查询优化失败: {e}")
            return sql, params

    def execute_with_cache(self, sql: str, params: Tuple = None) -> Any:
        """
        带缓存的查询执行

        Args:
            sql: SQL语句
            params: 查询参数

        Returns:
            Any: 查询结果
        """
        try:
            # 生成缓存键
            cache_key = self._generate_cache_key(sql, params)

            # 检查缓存
            if self.enable_query_cache and cache_key in self.query_cache:
                cache_entry = self.query_cache[cache_key]

                # 检查缓存是否过期
                if time.time() - cache_entry["timestamp"] < self.cache_ttl:
                    self.cache_stats["hits"] += 1
                    logger.debug(f"缓存命中: {cache_key[:50]}...")
                    return cache_entry["result"]
                else:
                    # 缓存过期，删除
                    del self.query_cache[cache_key]

            # 缓存未命中，执行查询
            self.cache_stats["misses"] += 1

            # 优化查询
            optimized_sql, optimized_params = self.optimize_query(sql, params)

            # 执行查询并计时
            start_time = time.time()
            result = self.db_manager.fetchall(optimized_sql, optimized_params)
            time.time() - start_time

            # 缓存结果
            if self.enable_query_cache:
                self._cache_result(cache_key, result)

            return result

        except Exception as e:
            logger.error(f"缓存查询执行失败: {e}")
            raise

    def analyze_query_performance(
        self, sql: str, params: Tuple = None
    ) -> Dict[str, Any]:
        """
        分析查询性能

        Args:
            sql: SQL语句
            params: 查询参数

        Returns:
            Dict[str, Any]: 性能分析结果
        """
        try:
            analysis = {
                "sql": sql,
                "params": params,
                "execution_time": 0,
                "rows_examined": 0,
                "rows_returned": 0,
                "index_usage": [],
                "suggestions": [],
            }

            # 执行EXPLAIN分析
            if self.enable_explain:
                explain_result = self._explain_query(sql, params)
                analysis.update(explain_result)

            # 执行查询并计时
            start_time = time.time()
            result = self.db_manager.fetchall(sql, params)
            analysis["execution_time"] = time.time() - start_time
            analysis["rows_returned"] = len(result) if result else 0

            # 生成优化建议
            analysis["suggestions"] = self._generate_optimization_suggestions(
                sql, analysis
            )

            return analysis

        except Exception as e:
            logger.error(f"查询性能分析失败: {e}")
            return {"error": str(e)}

    def suggest_indexes(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        建议索引策略

        Args:
            table_name: 表名（可选）

        Returns:
            List[Dict[str, Any]]: 索引建议列表
        """
        try:
            suggestions = []

            # 分析查询模式
            query_patterns = self._analyze_query_patterns()

            # 基于查询模式生成索引建议
            for pattern, frequency in query_patterns.items():
                if table_name and pattern.get("table") != table_name:
                    continue

                suggestion = self._generate_index_suggestion(pattern, frequency)
                if suggestion:
                    suggestions.append(suggestion)

            # 添加通用索引建议
            if not table_name or table_name == "daily_data":
                suggestions.extend(self._get_daily_data_index_suggestions())

            if not table_name or table_name == "stocks":
                suggestions.extend(self._get_stocks_index_suggestions())

            # 按优先级排序
            suggestions.sort(key=lambda x: x.get("priority", 0), reverse=True)

            return suggestions

        except Exception as e:
            logger.error(f"索引建议生成失败: {e}")
            return []

    def _analyze_query_pattern(self, sql: str) -> Dict[str, Any]:
        """分析查询模式"""
        pattern_info = {
            "has_date_range": False,
            "has_symbol_filter": False,
            "has_market_filter": False,
            "has_order_by": False,
            "has_limit": False,
            "table_name": None,
        }

        # 提取表名
        table_match = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
        if table_match:
            pattern_info["table_name"] = table_match.group(1)

        # 检查各种模式
        for pattern_name, pattern_regex in self.query_patterns.items():
            if pattern_regex.search(sql):
                pattern_info[f"has_{pattern_name}"] = True

        return pattern_info

    def _apply_optimization_rules(self, sql: str, query_info: Dict[str, Any]) -> str:
        """应用优化规则"""
        optimized_sql = sql

        # 规则1: 添加LIMIT子句（如果没有且是SELECT查询）
        if (
            "SELECT" in sql.upper()
            and not query_info["has_limit"]
            and "COUNT(" not in sql.upper()
        ):
            optimized_sql += " LIMIT 10000"

        # 规则2: 优化ORDER BY（添加索引提示）
        if query_info["has_order_by"] and query_info["table_name"] == "daily_data":
            # 建议使用复合索引
            pass

        # 规则3: 日期范围查询优化
        if query_info["has_date_range"]:
            # 确保日期格式正确
            optimized_sql = re.sub(
                r"trade_date\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'",
                r"trade_date BETWEEN '\1' AND '\2'",
                optimized_sql,
                flags=re.IGNORECASE,
            )

        return optimized_sql

    def _optimize_parameters(self, params: Tuple, query_info: Dict[str, Any]) -> Tuple:
        """优化查询参数"""
        if not params:
            return params

        # 参数类型转换和验证
        optimized_params = []
        for param in params:
            if isinstance(param, str):
                # 日期格式标准化
                if re.match(r"\d{4}-\d{2}-\d{2}", param):
                    optimized_params.append(param)
                else:
                    optimized_params.append(param)
            else:
                optimized_params.append(param)

        return tuple(optimized_params)

    def _generate_cache_key(self, sql: str, params: Tuple) -> str:
        """生成缓存键"""
        import hashlib

        # 标准化SQL（移除多余空格）
        normalized_sql = re.sub(r"\s+", " ", sql.strip())

        # 组合SQL和参数
        cache_content = f"{normalized_sql}|{params}"

        # 生成哈希
        return hashlib.md5(cache_content.encode()).hexdigest()

    def _cache_result(self, cache_key: str, result: Any):
        """缓存查询结果"""
        try:
            # 检查缓存大小限制
            if len(self.query_cache) >= self.max_cache_size:
                # LRU淘汰策略
                oldest_key = min(
                    self.query_cache.keys(),
                    key=lambda k: self.query_cache[k]["timestamp"],
                )
                del self.query_cache[oldest_key]
                self.cache_stats["evictions"] += 1

            # 添加到缓存
            self.query_cache[cache_key] = {"result": result, "timestamp": time.time()}

        except Exception as e:
            logger.error(f"缓存结果失败: {e}")

    def _explain_query(self, sql: str, params: Tuple) -> Dict[str, Any]:
        """执行EXPLAIN分析"""
        try:
            explain_sql = f"EXPLAIN QUERY PLAN {sql}"
            explain_result = self.db_manager.fetchall(explain_sql, params)

            analysis = {
                "explain_plan": explain_result,
                "uses_index": False,
                "scan_type": "unknown",
            }

            # 分析执行计划
            if explain_result:
                for row in explain_result:
                    detail = str(row).lower()
                    if "index" in detail:
                        analysis["uses_index"] = True
                    if "scan" in detail:
                        if "table" in detail:
                            analysis["scan_type"] = "table_scan"
                        elif "index" in detail:
                            analysis["scan_type"] = "index_scan"

            return analysis

        except Exception as e:
            logger.debug(f"EXPLAIN查询失败: {e}")
            return {}

    def _generate_optimization_suggestions(
        self, sql: str, analysis: Dict[str, Any]
    ) -> List[str]:
        """生成基本优化建议"""
        suggestions = []

        # 只保留最基本的建议
        if analysis.get("execution_time", 0) > self.slow_query_threshold:
            suggestions.append("查询执行时间较长，建议优化")

        if "SELECT *" in sql.upper():
            suggestions.append("避免使用SELECT *，建议指定具体字段")

        return suggestions

    def _analyze_query_patterns(self) -> Dict[str, int]:
        """分析查询模式频率"""
        patterns = defaultdict(int)

        for sql in self.query_stats.keys():
            pattern_info = self._analyze_query_pattern(sql)

            # 统计模式频率
            if pattern_info["has_date_range"]:
                patterns[f"{pattern_info['table_name']}_date_range"] += len(
                    self.query_stats[sql]
                )

            if pattern_info["has_symbol_filter"]:
                patterns[f"{pattern_info['table_name']}_symbol"] += len(
                    self.query_stats[sql]
                )

            if pattern_info["has_market_filter"]:
                patterns[f"{pattern_info['table_name']}_market"] += len(
                    self.query_stats[sql]
                )

        return dict(patterns)

    def _generate_index_suggestion(
        self, pattern: Dict[str, Any], frequency: int
    ) -> Dict[str, Any]:
        """生成索引建议"""
        # 基于模式和频率生成索引建议
        suggestion = {
            "pattern": pattern,
            "frequency": frequency,
            "priority": frequency * 10,  # 简单的优先级计算
            "index_type": "btree",
            "estimated_benefit": "high" if frequency > 100 else "medium",
        }

        return suggestion

    def _get_daily_data_index_suggestions(self) -> List[Dict[str, Any]]:
        """获取daily_data表的索引建议"""
        return [
            {
                "table": "daily_data",
                "columns": ["symbol", "trade_date"],
                "index_name": "idx_daily_data_symbol_date",
                "priority": 100,
                "reason": "股票代码和日期的复合查询最常见",
                "sql": "CREATE INDEX idx_daily_data_symbol_date ON daily_data(symbol, trade_date)",
            },
            {
                "table": "daily_data",
                "columns": ["trade_date"],
                "index_name": "idx_daily_data_date",
                "priority": 80,
                "reason": "日期范围查询优化",
                "sql": "CREATE INDEX idx_daily_data_date ON daily_data(trade_date)",
            },
            {
                "table": "daily_data",
                "columns": ["market"],
                "index_name": "idx_daily_data_market",
                "priority": 60,
                "reason": "市场筛选查询优化",
                "sql": "CREATE INDEX idx_daily_data_market ON daily_data(market)",
            },
        ]

    def _get_stocks_index_suggestions(self) -> List[Dict[str, Any]]:
        """获取stocks表的索引建议"""
        return [
            {
                "table": "stocks",
                "columns": ["market", "status"],
                "index_name": "idx_stocks_market_status",
                "priority": 90,
                "reason": "市场和状态的复合查询",
                "sql": "CREATE INDEX idx_stocks_market_status ON stocks(market, status)",
            },
            {
                "table": "stocks",
                "columns": ["list_date"],
                "index_name": "idx_stocks_list_date",
                "priority": 50,
                "reason": "上市日期查询优化",
                "sql": "CREATE INDEX idx_stocks_list_date ON stocks(list_date)",
            },
        ]

    def clear_cache(self):
        """清空查询缓存"""
        self.query_cache.clear()
        self.cache_stats = {"hits": 0, "misses": 0, "evictions": 0}
        logger.info("查询缓存已清空")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (
            self.cache_stats["hits"] / total_requests if total_requests > 0 else 0
        )

        return {
            "cache_size": len(self.query_cache),
            "max_cache_size": self.max_cache_size,
            "cache_ttl": self.cache_ttl,
            "hits": self.cache_stats["hits"],
            "misses": self.cache_stats["misses"],
            "evictions": self.cache_stats["evictions"],
            "hit_rate": hit_rate,
            "total_requests": total_requests,
        }

    def get_optimizer_stats(self) -> Dict[str, Any]:
        """获取优化器统计信息"""
        return {
            "optimizer_name": "SimTradeData Query Optimizer",
            "version": "1.0.0",
            "cache_stats": self.get_cache_stats(),
            "query_stats": {
                "total_queries": len(self.query_stats),
                "slow_queries": len(self.get_slow_queries()),
                "avg_execution_time": self._calculate_avg_execution_time(),
            },
            "optimization_features": [
                "Query Caching",
                "SQL Optimization",
                "Index Suggestions",
                "Performance Analysis",
                "Slow Query Detection",
            ],
        }

    def get_slow_queries(self, threshold: float = 1.0) -> List[str]:
        """获取慢查询列表"""
        try:
            slow_queries = []
            for sql, times in self.query_stats.items():
                avg_time = sum(times) / len(times) if times else 0.0
                if avg_time > threshold:
                    slow_queries.append(sql)
            return slow_queries
        except Exception:
            return []

    def _calculate_avg_execution_time(self) -> float:
        """计算平均执行时间"""
        try:
            all_times = []
            for stats_list in self.query_stats.values():
                all_times.extend(stats_list)

            return sum(all_times) / len(all_times) if all_times else 0.0

        except Exception:
            return 0.0
