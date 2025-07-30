"""
API网关

统一管理所有API接口，提供负载均衡、限流、认证等功能。
"""

import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Dict

from ..api import APIRouter
from ..config import Config
from ..database import DatabaseManager
from .ptrade_api import PTradeAPIAdapter
from .rest_api import RESTAPIServer

logger = logging.getLogger(__name__)


class APIGateway:
    """API网关"""

    def __init__(
        self, db_manager: DatabaseManager, api_router: APIRouter, config: Config = None
    ):
        """
        初始化API网关

        Args:
            db_manager: 数据库管理器
            api_router: API路由器
            config: 配置对象
        """
        self.db_manager = db_manager
        self.api_router = api_router
        self.config = config or Config()

        # 网关配置
        self.enable_rate_limiting = self.config.get(
            "api_gateway.enable_rate_limiting", True
        )
        self.rate_limit_requests = self.config.get(
            "api_gateway.rate_limit_requests", 1000
        )
        self.rate_limit_window = self.config.get(
            "api_gateway.rate_limit_window", 3600
        )  # 1小时
        self.enable_authentication = self.config.get(
            "api_gateway.enable_authentication", False
        )
        self.enable_logging = self.config.get("api_gateway.enable_logging", True)

        # 初始化API服务
        self.ptrade_api = PTradeAPIAdapter(db_manager, api_router, config)
        self.rest_api = RESTAPIServer(db_manager, api_router, config)

        # 限流管理
        self.request_counts = defaultdict(deque)  # {client_id: deque of timestamps}
        self.rate_limit_lock = threading.Lock()

        # 统计信息
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "rate_limited_requests": 0,
            "start_time": datetime.now(),
        }

        # API密钥管理 (简化版)
        self.api_keys = self.config.get("api_gateway.api_keys", {})

        logger.info("API网关初始化完成")

    def start_all_services(self):
        """启动所有API服务"""
        try:
            # 启动REST API
            self.rest_api.start(threaded=True)

            logger.info("所有API服务启动完成")

        except Exception as e:
            logger.error(f"启动API服务失败: {e}")
            raise

    def stop_all_services(self):
        """停止所有API服务"""
        try:
            # 停止REST API
            self.rest_api.stop()

            logger.info("所有API服务已停止")

        except Exception as e:
            logger.error(f"停止API服务失败: {e}")

    def is_request_allowed(self, client_id: str) -> bool:
        """
        检查客户端请求是否被允许（未超过限流阈值）

        Args:
            client_id: 客户端ID

        Returns:
            bool: True表示允许请求，False表示被限流
        """
        if not self.enable_rate_limiting:
            return True

        try:
            with self.rate_limit_lock:
                now = time.time()
                window_start = now - self.rate_limit_window

                # 获取客户端请求记录
                requests = self.request_counts[client_id]

                # 清理过期记录
                while requests and requests[0] < window_start:
                    requests.popleft()

                # 检查是否超过限制
                if len(requests) >= self.rate_limit_requests:
                    self.stats["rate_limited_requests"] += 1
                    return False

                # 记录当前请求
                requests.append(now)
                return True

        except Exception as e:
            logger.error(f"检查限流失败: {e}")
            return True  # 出错时允许请求

    def authenticate_request(self, api_key: str) -> bool:
        """
        验证API请求

        Args:
            api_key: API密钥

        Returns:
            bool: True表示验证通过，False表示验证失败
        """
        if not self.enable_authentication:
            return True

        if not api_key:
            return False

        return api_key in self.api_keys

    def log_request(
        self,
        client_id: str,
        endpoint: str,
        method: str,
        status: str,
        response_time: float = None,
    ):
        """
        记录API请求日志

        Args:
            client_id: 客户端ID
            endpoint: 请求端点
            method: 请求方法
            status: 请求状态
            response_time: 响应时间
        """
        if not self.enable_logging:
            return

        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "client_id": client_id,
                "endpoint": endpoint,
                "method": method,
                "status": status,
                "response_time": response_time,
            }

            # 更新统计
            self.stats["total_requests"] += 1
            if status == "success":
                self.stats["successful_requests"] += 1
            else:
                self.stats["failed_requests"] += 1

            # 记录到日志
            logger.info(f"API请求: {log_entry}")

            # 可以扩展为写入数据库或文件

        except Exception as e:
            logger.error(f"记录请求日志失败: {e}")

    def process_request(
        self, client_id: str, api_key: str, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        处理API请求

        Args:
            client_id: 客户端ID
            api_key: API密钥
            request_data: 请求数据

        Returns:
            Dict[str, Any]: 响应数据
        """
        start_time = time.time()

        try:
            # 1. 认证检查
            if not self.authenticate_request(api_key):
                response = {
                    "success": False,
                    "error": "Authentication failed",
                    "error_code": "AUTH_FAILED",
                }
                self.log_request(
                    client_id,
                    request_data.get("endpoint", ""),
                    request_data.get("method", ""),
                    "auth_failed",
                )
                return response

            # 2. 限流检查
            if not self.is_request_allowed(client_id):
                response = {
                    "success": False,
                    "error": "Rate limit exceeded",
                    "error_code": "RATE_LIMITED",
                }
                self.log_request(
                    client_id,
                    request_data.get("endpoint", ""),
                    request_data.get("method", ""),
                    "rate_limited",
                )
                return response

            # 3. 处理请求
            api_type = request_data.get("api_type", "rest")

            if api_type == "ptrade":
                # PTrade API兼容层
                result = self._process_ptrade_request(request_data)
            elif api_type == "rest":
                # REST API
                result = self._process_rest_request(request_data)
            else:
                result = {
                    "success": False,
                    "error": f"Unsupported API type: {api_type}",
                    "error_code": "UNSUPPORTED_API",
                }

            # 4. 记录日志
            response_time = time.time() - start_time
            status = "success" if result.get("success", False) else "failed"
            self.log_request(
                client_id,
                request_data.get("endpoint", ""),
                request_data.get("method", ""),
                status,
                response_time,
            )

            return result

        except Exception as e:
            logger.error(f"处理API请求失败: {e}")
            response_time = time.time() - start_time
            self.log_request(
                client_id,
                request_data.get("endpoint", ""),
                request_data.get("method", ""),
                "error",
                response_time,
            )

            return {"success": False, "error": str(e), "error_code": "INTERNAL_ERROR"}

    def _process_ptrade_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理PTrade API请求"""
        try:
            method = request_data.get("method")
            params = request_data.get("params", {})

            if method == "get_stock_list":
                result = self.ptrade_api.get_stock_list(**params)
                return {
                    "success": True,
                    "data": (
                        result.to_dict("records")
                        if hasattr(result, "to_dict")
                        else result
                    ),
                }
            elif method == "get_price":
                result = self.ptrade_api.get_price(**params)
                return {
                    "success": True,
                    "data": (
                        result.to_dict("records")
                        if hasattr(result, "to_dict")
                        else result
                    ),
                }
            elif method == "get_fundamentals":
                result = self.ptrade_api.get_fundamentals(**params)
                return {
                    "success": True,
                    "data": (
                        result.to_dict("records")
                        if hasattr(result, "to_dict")
                        else result
                    ),
                }
            else:
                return {
                    "success": False,
                    "error": f"Unsupported PTrade method: {method}",
                    "error_code": "UNSUPPORTED_METHOD",
                }

        except Exception as e:
            return {"success": False, "error": str(e), "error_code": "PTRADE_ERROR"}

    def _process_rest_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理REST API请求"""
        try:
            # REST API请求通过Flask处理，这里主要是路由
            query_params = request_data.get("params", {})
            result = self.api_router.query(query_params)

            return {"success": True, "data": result}

        except Exception as e:
            return {"success": False, "error": str(e), "error_code": "REST_ERROR"}

    def get_gateway_stats(self) -> Dict[str, Any]:
        """获取网关统计信息"""
        uptime = datetime.now() - self.stats["start_time"]

        return {
            "gateway_info": {
                "name": "SimTradeData API Gateway",
                "version": "1.0.0",
                "uptime_seconds": uptime.total_seconds(),
                "uptime_formatted": str(uptime),
            },
            "request_stats": self.stats.copy(),
            "rate_limiting": {
                "enabled": self.enable_rate_limiting,
                "requests_per_window": self.rate_limit_requests,
                "window_seconds": self.rate_limit_window,
                "active_clients": len(self.request_counts),
            },
            "authentication": {
                "enabled": self.enable_authentication,
                "registered_keys": len(self.api_keys),
            },
            "services": {
                "ptrade_api": {
                    "status": "active",
                    "info": self.ptrade_api.get_adapter_info(),
                },
                "rest_api": {
                    "status": "active" if self.rest_api.is_running else "inactive",
                    "info": self.rest_api.get_server_info(),
                },
            },
        }

    def add_api_key(self, key: str, description: str = ""):
        """添加API密钥"""
        self.api_keys[key] = {
            "description": description,
            "created_time": datetime.now().isoformat(),
            "last_used": None,
        }
        logger.info(f"添加API密钥: {key[:8]}...")

    def remove_api_key(self, key: str):
        """移除API密钥"""
        if key in self.api_keys:
            del self.api_keys[key]
            logger.info(f"移除API密钥: {key[:8]}...")

    def cleanup_rate_limit_data(self):
        """清理过期的限流数据"""
        try:
            with self.rate_limit_lock:
                now = time.time()
                window_start = now - self.rate_limit_window

                clients_to_remove = []
                for client_id, requests in self.request_counts.items():
                    # 清理过期记录
                    while requests and requests[0] < window_start:
                        requests.popleft()

                    # 如果没有活跃请求，标记为删除
                    if not requests:
                        clients_to_remove.append(client_id)

                # 删除无活跃请求的客户端
                for client_id in clients_to_remove:
                    del self.request_counts[client_id]

                logger.debug(
                    f"清理限流数据: 删除 {len(clients_to_remove)} 个非活跃客户端"
                )

        except Exception as e:
            logger.error(f"清理限流数据失败: {e}")

    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 检查各个服务状态
            services_status = {
                "ptrade_api": True,  # PTrade API适配器总是可用
                "rest_api": self.rest_api.is_running,
                "database": True,  # 简化检查
                "api_router": True,  # 简化检查
            }

            all_healthy = all(services_status.values())

            return {
                "status": "healthy" if all_healthy else "degraded",
                "timestamp": datetime.now().isoformat(),
                "services": services_status,
                "uptime": (datetime.now() - self.stats["start_time"]).total_seconds(),
                "total_requests": self.stats["total_requests"],
            }

        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
            }
