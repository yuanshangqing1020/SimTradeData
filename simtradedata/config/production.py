"""
生产环境配置

针对生产环境优化的配置参数。
"""

from typing import Any, Dict

# 生产环境配置
PRODUCTION_CONFIG = {
    # 数据库配置 - 生产级优化
    "database": {
        "path": "/var/lib/simtradedata/simtradedata.db",  # 生产路径
        "timeout": 60.0,  # 增加超时
        "check_same_thread": False,  # 多线程支持
        "backup_enabled": True,
        "backup_interval_hours": 12,  # 每12小时备份
        "backup_path": "/var/backups/simtradedata",  # 独立备份目录
        "vacuum_interval_days": 7,
        # SQLite性能优化
        "pragma": {
            "journal_mode": "WAL",  # Write-Ahead Logging模式
            "synchronous": "NORMAL",  # 平衡性能和安全
            "cache_size": -64000,  # 64MB缓存（负数表示KB）
            "temp_store": "MEMORY",  # 临时表存储在内存
            "mmap_size": 268435456,  # 256MB内存映射
            "page_size": 4096,  # 4KB页大小
            "busy_timeout": 30000,  # 30秒繁忙超时
        },
    },
    # 市场配置 - 生产环境启用所有市场
    "markets": {
        "enabled": ["SZ", "SS", "HK", "US"],
    },
    # 数据源配置 - 生产环境优化
    "data_sources": {
        "mootdx": {
            "enabled": True,
            "tdx_dir": "/mnt/c/new_tdx",
            "use_online": True,
            "market": "std",
            "timeout": 30,  # 增加超时
            "rate_limit": 300,  # 限制请求频率
        },
        "baostock": {
            "enabled": True,
            "timeout": 30,
            "retry_times": 5,
            "retry_delay": 3,
            "rate_limit": 300,
            "connection_pool_size": 10,
        },
        "qstock": {
            "enabled": False,  # 生产环境暂时禁用（依赖问题）
            "timeout": 30,
            "retry_times": 5,
            "retry_delay": 3,
            "rate_limit": 200,
        },
    },
    # 数据同步配置 - 生产环境优化
    "sync": {
        "enabled": True,
        "daily_schedule": "02:00",  # 凌晨2点同步
        "auto_gap_fix": True,
        "max_gap_days": 90,  # 增加到90天
        "max_concurrent_tasks": 3,  # 适度并行
        "batch_size": 50,  # 增加批次大小
        "minute_data_retention_days": 90,  # 保留90天分钟数据
        "daily_data_retention_days": 3650,  # 保留10年日线数据
        "enable_parallel_download": True,  # 启用并行下载
        "enable_parallel_processing": True,
        "max_processing_workers": 8,  # 更多处理线程
        "max_download_workers": 3,  # 下载线程
        "download_rate_limit": 100,  # 每秒下载限制
    },
    # 查询性能配置 - 生产环境优化
    "query": {
        "default_limit": 5000,  # 增加默认限制
        "max_limit": 50000,  # 增加最大限制
        "cache_enabled": True,
        "cache_ttl_seconds": 600,  # 缓存10分钟
        "cache_max_size": 10000,  # 最多缓存10000个查询
        "parallel_query_enabled": True,  # 启用并行查询
        "max_parallel_queries": 4,  # 最多4个并行查询
        "query_timeout_seconds": 30,  # 查询超时
    },
    # 日志配置 - 生产环境优化
    "logging": {
        "level": "WARNING",  # 生产环境只记录WARNING及以上
        "format": "%(asctime)s - %(name)s - %(levelname)s - [%(process)d:%(thread)d] - %(message)s",
        "file_enabled": True,
        "file_path": "/var/log/simtradedata/simtradedata.log",
        "file_max_size": "100MB",  # 增加日志文件大小
        "file_backup_count": 10,  # 保留更多日志文件
        "console_enabled": False,  # 生产环境禁用控制台输出
        # 结构化日志
        "structured_logging": True,
        "json_format": True,
        # 日志分级
        "separate_error_log": True,
        "error_log_path": "/var/log/simtradedata/error.log",
        # 性能日志
        "performance_log_enabled": True,
        "performance_log_path": "/var/log/simtradedata/performance.log",
    },
    # 监控配置 - 生产环境启用
    "monitoring": {
        "enabled": True,
        "slow_query_threshold": 500,  # 500ms慢查询阈值
        "metrics_enabled": True,
        "metrics_export_interval": 60,  # 每分钟导出指标
        "metrics_path": "/var/lib/simtradedata/metrics",
        # 健康检查
        "health_check_enabled": True,
        "health_check_interval": 30,  # 30秒检查一次
        # 告警配置
        "alert_enabled": True,
        "alert_rules": [
            "data_quality_check",
            "sync_failure_check",
            "database_size_check",
            "missing_data_check",
            "stale_data_check",
        ],
        "alert_check_interval": 300,  # 5分钟检查一次
    },
    # API配置 - 生产环境优化
    "api": {
        "enabled_apis": [
            # 所有PTrade API
            "get_history",
            "get_price",
            "get_Ashares",
            "get_stock_info",
            "get_trade_days",
            "get_all_trade_days",
            "get_fundamentals",
            "get_stock_blocks",
            "get_snapshot",
            "get_realtime_quotes",
            "get_technical_indicators",
        ],
        "rate_limit_enabled": True,
        "rate_limit_per_minute": 5000,  # 增加到5000
        "rate_limit_per_hour": 100000,  # 每小时限制
        "response_format": "pandas",
        "error_handling": "strict",
        # 并发控制
        "max_concurrent_requests": 50,
        # 响应压缩
        "compression_enabled": True,
        # 结果缓存
        "result_cache_enabled": True,
        "result_cache_size": 5000,
    },
    # 扩展功能配置
    "extensions": {
        "technical_indicators_enabled": True,
        "custom_fields_enabled": True,
        "data_export_enabled": True,
        "backup_restore_enabled": True,
    },
    # 安全配置
    "security": {
        "ssl_enabled": False,  # 如需HTTPS启用
        "api_key_enabled": False,  # API密钥认证
        "ip_whitelist_enabled": False,  # IP白名单
        "request_signing_enabled": False,  # 请求签名
    },
    # 性能优化配置
    "performance": {
        # 数据预热
        "preload_hot_data": True,  # 启动时预加载热数据
        "hot_symbols": [],  # 热门股票列表，留空则自动识别
        # 索引优化
        "auto_create_indexes": True,
        "index_maintenance_enabled": True,
        # 压缩
        "data_compression_enabled": False,  # SQLite本身已经优化
        # 内存管理
        "max_memory_mb": 4096,  # 最大内存使用4GB
    },
    # 技术指标配置
    "technical_indicators": {
        "ma": {"enabled": True, "periods": [5, 10, 20, 60, 120, 250]},
        "ema": {"enabled": True, "periods": [12, 26, 50, 200]},
        "macd": {"enabled": True, "fast": 12, "slow": 26, "signal": 9},
        "rsi": {"enabled": True, "period": 14},
        "kdj": {"enabled": True, "k_period": 9, "d_period": 3},
        # 缓存配置
        "cache_enabled": True,
        "cache_size": 5000,  # 增加缓存大小
    },
}


def get_production_config() -> Dict[str, Any]:
    """获取生产环境配置"""
    return PRODUCTION_CONFIG.copy()


def merge_configs(
    base_config: Dict[str, Any], override_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    深度合并配置字典

    Args:
        base_config: 基础配置
        override_config: 覆盖配置

    Returns:
        Dict[str, Any]: 合并后的配置
    """
    result = base_config.copy()

    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value

    return result
