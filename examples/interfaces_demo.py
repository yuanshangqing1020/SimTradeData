"""
用户接口层演示

展示PTrade API兼容层、RESTful API、WebSocket API和API网关功能。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from unittest.mock import Mock

import pandas as pd

from simtradedata.config import Config
from simtradedata.database import DatabaseManager
from simtradedata.interfaces import APIGateway, PTradeAPIAdapter, RESTAPIServer

# 设置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def demo_ptrade_api_adapter():
    """演示PTrade API适配器"""
    print("\n🔌 PTrade API适配器演示")
    print("=" * 50)

    # 创建模拟组件
    db_manager = Mock(spec=DatabaseManager)
    api_router = Mock()
    config = Config()

    # 模拟API路由器返回股票列表
    api_router.query.return_value = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "stock_name": "平安银行",
                "market": "SZ",
                "status": "active",
                "list_date": "1991-04-03",
            },
            {
                "symbol": "000002.SZ",
                "stock_name": "万科A",
                "market": "SZ",
                "status": "active",
                "list_date": "1991-01-29",
            },
            {
                "symbol": "600000.SS",
                "stock_name": "浦发银行",
                "market": "SS",
                "status": "active",
                "list_date": "1999-11-10",
            },
            {
                "symbol": "600036.SS",
                "stock_name": "招商银行",
                "market": "SS",
                "status": "active",
                "list_date": "2002-04-09",
            },
            {
                "symbol": "00700.HK",
                "stock_name": "腾讯控股",
                "market": "HK",
                "status": "active",
                "list_date": "2004-06-16",
            },
            {
                "symbol": "AAPL.US",
                "stock_name": "Apple Inc.",
                "market": "US",
                "status": "active",
                "list_date": "1980-12-12",
            },
        ]
    )

    # 创建PTrade API适配器
    adapter = PTradeAPIAdapter(db_manager, api_router, config)

    print(f"🔧 PTrade API适配器配置:")
    adapter_info = adapter.get_adapter_info()
    print(f"  适配器名称: {adapter_info['adapter_name']}")
    print(f"  版本: {adapter_info['version']}")
    print(f"  兼容API: {len(adapter_info['compatible_apis'])} 个")
    print(f"  支持市场: {adapter_info['supported_markets']}")
    print(f"  支持频率: {adapter_info['supported_frequencies']}")

    # 测试获取股票列表
    print(f"\n📋 获取股票列表 (兼容PTrade格式):")
    stock_list = adapter.get_stock_list()

    print(f"  返回类型: {type(stock_list)}")
    print(f"  股票数量: {len(stock_list)}")
    print(f"  列名: {list(stock_list.columns)}")

    # 显示前几只股票
    print(f"  前5只股票:")
    for idx, row in stock_list.head().iterrows():
        print(f"    {row['code']} - {row['name']} ({row['market']})")

    # 模拟价格数据返回
    api_router.query.return_value = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-15",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 12500000,
                "money": 127500000,
                "change": 0.2,
                "change_percent": 2.0,
            },
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-16",
                "open": 10.2,
                "high": 10.8,
                "low": 10.0,
                "close": 10.5,
                "volume": 15000000,
                "money": 157500000,
                "change": 0.3,
                "change_percent": 2.94,
            },
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-17",
                "open": 10.5,
                "high": 10.6,
                "low": 10.1,
                "close": 10.3,
                "volume": 11000000,
                "money": 113300000,
                "change": -0.2,
                "change_percent": -1.90,
            },
        ]
    )

    # 测试获取价格数据
    print(f"\n💰 获取价格数据 (兼容PTrade格式):")
    price_data = adapter.get_price("000001.SZ", "2024-01-15", "2024-01-17")

    print(f"  返回类型: {type(price_data)}")
    print(f"  数据点数: {len(price_data)}")
    print(f"  列名: {list(price_data.columns)}")

    # 显示价格数据
    if not price_data.empty:
        print(f"  价格数据:")
        for idx, row in price_data.iterrows():
            if isinstance(idx, tuple):
                date_str = (
                    idx[1].strftime("%Y-%m-%d")
                    if hasattr(idx[1], "strftime")
                    else str(idx[1])
                )
                print(
                    f"    {date_str}: 开盘={row['open']}, 收盘={row['close']}, 涨跌幅={row['pct_change']:.2f}%"
                )
            else:
                print(f"    {idx}: 开盘={row['open']}, 收盘={row['close']}")

    # 模拟基本面数据
    api_router.query.return_value = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "pe": 5.2,
                "pb": 0.8,
                "ps": 1.5,
                "market_cap": 350000000000,
                "total_share": 19405918198,
                "float_share": 19405918198,
            }
        ]
    )

    # 测试获取基本面数据
    print(f"\n📊 获取基本面数据 (兼容PTrade格式):")
    fundamentals = adapter.get_fundamentals("000001.SZ")

    if not fundamentals.empty:
        row = fundamentals.iloc[0]
        print(f"  股票代码: {row['code']}")
        print(f"  市盈率: {row['pe_ratio']}")
        print(f"  市净率: {row['pb_ratio']}")
        print(f"  市销率: {row['ps_ratio']}")
        print(f"  总市值: ¥{row['market_cap']:,}")
        print(f"  总股本: {row['total_shares']:,}")

    print(f"\n✅ PTrade API适配器演示完成")


def demo_rest_api_server():
    """演示RESTful API服务器"""
    print("\n🌐 RESTful API服务器演示")
    print("=" * 50)

    # 创建模拟组件
    db_manager = Mock(spec=DatabaseManager)
    api_router = Mock()
    config = Config()

    # 设置测试端口
    config.set("rest_api.port", 8888)
    config.set("rest_api.debug", True)
    config.set("rest_api.enable_cors", True)

    # 模拟API路由器返回
    # 创建REST API服务器
    server = RESTAPIServer(db_manager, api_router, config)

    print(f"🔧 RESTful API服务器配置:")
    server_info = server.get_server_info()
    print(f"  服务器名称: {server_info['server_name']}")
    print(f"  版本: {server_info['version']}")
    print(f"  监听地址: {server_info['host']}:{server_info['port']}")
    print(f"  运行状态: {'运行中' if server_info['is_running'] else '未运行'}")
    print(f"  调试模式: {server_info['debug']}")
    print(f"  CORS支持: {server_info['enable_cors']}")

    print(f"\n📡 支持的API端点:")
    for endpoint in server_info["endpoints"]:
        print(f"    {endpoint}")

    print(f"\n🚀 API端点说明:")
    endpoints_desc = {
        "GET /api/v1/health": "健康检查",
        "GET /api/v1/stocks": "获取股票列表",
        "GET /api/v1/stocks/{symbol}": "获取单个股票详情",
        "GET /api/v1/stocks/{symbol}/history": "获取历史行情",
        "GET /api/v1/stocks/{symbol}/fundamentals": "获取基本面数据",
        "GET /api/v1/stocks/{symbol}/snapshot": "获取当日快照",
        "GET /api/v1/snapshots": "批量获取快照",
        "GET /api/v1/meta/stats": "查看API运行状态",
    }

    for endpoint, desc in endpoints_desc.items():
        print(f"    {endpoint}: {desc}")

    print(f"\n📝 使用示例:")
    print(f"    curl http://localhost:8888/api/v1/health")
    print(f"    curl http://localhost:8888/api/v1/stocks?market=SZ&limit=10")
    print(
        f"    curl http://localhost:8888/api/v1/stocks/000001.SZ/history?start_date=2024-01-01"
    )
    print(f"    curl http://localhost:8888/api/v1/stocks/000001.SZ/fundamentals")

    print(f"\n✅ RESTful API服务器演示完成")


def demo_api_gateway():
    """演示API网关"""
    print("\n🚪 API网关演示")
    print("=" * 50)

    # 创建模拟组件
    db_manager = Mock(spec=DatabaseManager)
    api_router = Mock()
    config = Config()

    # 设置网关配置
    config.set("api_gateway.enable_rate_limiting", True)
    config.set("api_gateway.rate_limit_requests", 100)
    config.set("api_gateway.rate_limit_window", 3600)
    config.set("api_gateway.enable_authentication", True)
    config.set("api_gateway.enable_logging", True)

    # 模拟API路由器返回
    api_router.query.return_value = {"result": "success", "data": []}

    # 创建API网关
    gateway = APIGateway(db_manager, api_router, config)

    print(f"🔧 API网关配置:")
    gateway_stats = gateway.get_gateway_stats()
    gateway_info = gateway_stats["gateway_info"]
    print(f"  网关名称: {gateway_info['name']}")
    print(f"  版本: {gateway_info['version']}")
    print(f"  运行时间: {gateway_info['uptime_formatted']}")

    # 限流配置
    rate_limiting = gateway_stats["rate_limiting"]
    print(f"\n🚦 限流配置:")
    print(f"  启用状态: {rate_limiting['enabled']}")
    print(f"  请求限制: {rate_limiting['requests_per_window']} 次/小时")
    print(f"  时间窗口: {rate_limiting['window_seconds']} 秒")
    print(f"  活跃客户端: {rate_limiting['active_clients']}")

    # 认证配置
    authentication = gateway_stats["authentication"]
    print(f"\n🔐 认证配置:")
    print(f"  启用状态: {authentication['enabled']}")
    print(f"  注册密钥: {authentication['registered_keys']} 个")

    # 服务状态
    services = gateway_stats["services"]
    print(f"\n🔧 服务状态:")
    for service_name, service_info in services.items():
        status = service_info["status"]
        print(f"  {service_name}: {status}")

    # 测试API密钥管理
    print(f"\n🔑 API密钥管理:")
    test_key = "demo_api_key_12345"
    gateway.add_api_key(test_key, "演示用API密钥")
    print(f"  添加密钥: {test_key[:12]}...")

    # 测试认证
    print(f"  认证测试:")
    print(f"    有效密钥: {gateway.authenticate_request(test_key)}")
    print(f"    无效密钥: {gateway.authenticate_request('invalid_key')}")

    # 测试限流
    print(f"\n🚦 限流测试:")
    client_id = "demo_client"

    # 连续请求测试
    success_count = 0
    for i in range(5):
        if gateway.is_request_allowed(client_id):
            success_count += 1

    print(f"  5次请求中通过: {success_count} 次")

    # 测试请求处理
    print(f"\n📨 请求处理测试:")

    # PTrade API请求
    ptrade_request = {
        "api_type": "ptrade",
        "method": "get_stock_list",
        "params": {"market": "SZ"},
        "endpoint": "/ptrade/get_stock_list",
        "method": "GET",
    }

    result = gateway.process_request(client_id, test_key, ptrade_request)
    print(f"  PTrade API请求: {'成功' if result.get('success') else '失败'}")

    # REST API请求
    rest_request = {
        "api_type": "rest",
        "params": {"data_type": "stock_list"},
        "endpoint": "/api/v1/stocks",
        "method": "GET",
    }

    result = gateway.process_request(client_id, test_key, rest_request)
    print(f"  REST API请求: {'成功' if result.get('success') else '失败'}")

    # 测试健康检查
    print(f"\n💊 健康检查:")
    health = gateway.health_check()
    print(f"  整体状态: {health['status']}")
    print(f"  总请求数: {health['total_requests']}")
    print(f"  运行时间: {health['uptime']:.1f} 秒")

    # 显示服务状态
    print(f"  服务状态:")
    for service, status in health["services"].items():
        print(f"    {service}: {'✅' if status else '❌'}")

    # 清理
    gateway.remove_api_key(test_key)
    print(f"\n🧹 清理: 移除演示API密钥")

    print(f"\n✅ API网关演示完成")


def main():
    """主演示函数"""
    print("🚀 SimTradeData 用户接口层演示")
    print("=" * 60)

    try:
        # 演示各个组件
        demo_ptrade_api_adapter()
        demo_rest_api_server()
        demo_api_gateway()

        print("\n🎉 用户接口层演示完成!")
        print("\n📝 总结:")
        print("✅ PTrade API适配器: 完美兼容PTrade原生API，无缝迁移")
        print("✅ RESTful API服务器: 标准HTTP接口，支持CORS跨域")
        print("✅ API网关: 统一入口，限流认证，负载均衡")
        print("✅ 多协议支持: HTTP/HTTPS、PTrade原生")
        print("✅ 企业级特性: 认证、限流、日志、监控、健康检查")

        print("\n🌐 部署建议:")
        print("  开发环境: 使用PTrade API适配器，快速开发调试")
        print("  生产环境: 使用API网关 + REST API，提供企业级服务")
        print("  混合架构: 多种接口并存，满足不同场景需求")

    except Exception as e:
        logger.error(f"演示过程中出现错误: {e}")
        raise


if __name__ == "__main__":
    main()
