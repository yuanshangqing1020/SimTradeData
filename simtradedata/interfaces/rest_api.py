"""
RESTful API服务器 (FastAPI 版本)

提供基于FastAPI的REST接口，统一路径语义并自动生成OpenAPI文档。
"""

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ..api import APIRouter
from ..api.middleware import (
    AuthenticationMiddleware,
    RateLimitMiddleware,
    RequestLoggingMiddleware,
)
from ..config import Config
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class RESTAPIServer:
    """基于 FastAPI 的 RESTful 服务封装"""

    def __init__(
        self,
        db_manager: DatabaseManager,
        api_router: APIRouter,
        config: Config = None,
    ) -> None:
        self.db_manager = db_manager
        self.api_router = api_router
        self.config = config or Config()

        self.host = self.config.get("rest_api.host", "0.0.0.0")
        self.port = self.config.get("rest_api.port", 8080)
        self.debug = self.config.get("rest_api.debug", False)
        self.enable_cors = self.config.get("rest_api.enable_cors", True)
        self.docs_url = self.config.get("rest_api.docs_url", "/docs")
        self.redoc_url = self.config.get("rest_api.redoc_url", "/redoc")

        # Middleware configuration
        self.enable_rate_limiting = self.config.get(
            "api_gateway.enable_rate_limiting", True
        )
        self.rate_limit_requests = self.config.get(
            "api_gateway.rate_limit_requests", 1000
        )
        self.rate_limit_window = self.config.get("api_gateway.rate_limit_window", 3600)
        self.enable_authentication = self.config.get(
            "api_gateway.enable_authentication", False
        )
        self.enable_logging = self.config.get("api_gateway.enable_logging", True)
        self.api_keys = self.config.get("api_gateway.api_keys", {})

        self.app = self._create_app()
        self._server: Optional[uvicorn.Server] = None
        self.server_thread: Optional[threading.Thread] = None
        self.is_running = False

        # Store middleware references for statistics
        self.rate_limiter: Optional[RateLimitMiddleware] = None
        self.authenticator: Optional[AuthenticationMiddleware] = None
        self.request_logger: Optional[RequestLoggingMiddleware] = None

        logger.info("FastAPI REST server initialized @ %s:%s", self.host, self.port)

    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title="SimTradeData REST API",
            description="Unified REST interface for SimTradeData built on FastAPI",
            version="1.0.0",
            docs_url=self.docs_url,
            redoc_url=self.redoc_url,
        )

        # Add middleware in reverse order (last added = first executed)

        # 1. Request logging (outermost - logs all requests)
        if self.enable_logging:
            self.request_logger = RequestLoggingMiddleware(
                app,
                enabled=True,
                log_request_body=False,
                log_response_body=False,
            )
            app.add_middleware(
                RequestLoggingMiddleware,
                enabled=True,
                log_request_body=False,
                log_response_body=False,
            )
            logger.info("Request logging middleware enabled")

        # 2. Rate limiting
        if self.enable_rate_limiting:
            self.rate_limiter = RateLimitMiddleware(
                app,
                enabled=True,
                max_requests=self.rate_limit_requests,
                window_seconds=self.rate_limit_window,
            )
            app.add_middleware(
                RateLimitMiddleware,
                enabled=True,
                max_requests=self.rate_limit_requests,
                window_seconds=self.rate_limit_window,
            )
            logger.info(
                "Rate limiting middleware enabled: %d req/%ds",
                self.rate_limit_requests,
                self.rate_limit_window,
            )

        # 3. Authentication
        if self.enable_authentication:
            self.authenticator = AuthenticationMiddleware(
                app,
                enabled=True,
                api_keys=self.api_keys,
                public_paths=[
                    "/docs",
                    "/redoc",
                    "/openapi.json",
                    "/api/v1/health",
                ],
            )
            app.add_middleware(
                AuthenticationMiddleware,
                enabled=True,
                api_keys=self.api_keys,
                public_paths=[
                    "/docs",
                    "/redoc",
                    "/openapi.json",
                    "/api/v1/health",
                ],
            )
            logger.info(
                "Authentication middleware enabled with %d API keys", len(self.api_keys)
            )

        # 4. CORS (innermost)
        if self.enable_cors:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )
            logger.info("CORS middleware enabled")

        self._register_routes(app)
        return app

    # ------------------------------------------------------------------
    # 路由注册
    # ------------------------------------------------------------------
    def _register_routes(self, app: FastAPI) -> None:
        @app.get("/api/v1/health", tags=["system"])
        def health() -> Dict[str, Any]:
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": "1.0.0",
            }

        @app.get("/api/v1/stocks", tags=["stocks"])
        def list_stocks(
            market: Optional[str] = Query(
                default=None, description="市场过滤，例如 SZ/SS/HK/US"
            ),
            status: str = Query(default="active", description="股票状态"),
            industry: Optional[str] = Query(default=None, description="行业过滤"),
            fields: Optional[str] = Query(
                default=None, description="以逗号分隔的字段列表"
            ),
            limit: Optional[int] = Query(
                default=1000, ge=1, le=5000, description="返回条数"
            ),
            offset: int = Query(default=0, ge=0, description="偏移量"),
            use_cache: bool = Query(default=True, description="是否使用缓存"),
        ) -> Dict[str, Any]:
            field_list = self._parse_csv(fields)
            result = self.api_router.get_stock_info(
                market=market,
                industry=industry,
                status=status,
                fields=field_list,
                format_type="dict",
                limit=limit,
                offset=offset,
                use_cache=use_cache,
            )
            return self._ensure_success(result)

        @app.get("/api/v1/stocks/{symbol}", tags=["stocks"])
        def get_stock_detail(symbol: str) -> Dict[str, Any]:
            result = self.api_router.get_stock_info(
                symbols=symbol,
                format_type="dict",
                limit=1,
                use_cache=True,
            )
            payload = self._ensure_success(result)
            data = payload.get("data", [])
            if not data:
                raise HTTPException(status_code=404, detail="Symbol not found")
            return data[0]

        @app.get("/api/v1/stocks/{symbol}/history", tags=["history"])
        def get_stock_history(
            symbol: str,
            start_date: Optional[str] = Query(
                default=None, description="开始日期 YYYY-MM-DD"
            ),
            end_date: Optional[str] = Query(
                default=None, description="结束日期 YYYY-MM-DD"
            ),
            frequency: str = Query(default="1d", description="频率 1d/5m/15m/30m/60m"),
            fields: Optional[str] = Query(default=None, description="字段列表"),
            limit: Optional[int] = Query(default=None, ge=1, le=10000),
            offset: int = Query(default=0, ge=0),
            use_cache: bool = Query(default=True),
        ) -> Dict[str, Any]:
            field_list = self._parse_csv(fields)
            result = self.api_router.get_history(
                symbols=symbol,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                fields=field_list,
                format_type="dict",
                limit=limit,
                offset=offset,
                use_cache=use_cache,
            )
            return self._ensure_success(result)

        @app.get("/api/v1/stocks/{symbol}/fundamentals", tags=["fundamentals"])
        def get_stock_fundamentals(
            symbol: str,
            report_date: Optional[str] = Query(default=None, description="报告期"),
            report_type: Optional[str] = Query(default=None, description="报告类型"),
            fields: Optional[str] = Query(default=None, description="字段列表"),
            limit: Optional[int] = Query(default=None, ge=1, le=5000),
            offset: int = Query(default=0, ge=0),
            use_cache: bool = Query(default=True),
        ) -> Dict[str, Any]:
            field_list = self._parse_csv(fields)
            result = self.api_router.get_fundamentals(
                symbols=symbol,
                report_date=report_date,
                report_type=report_type,
                fields=field_list,
                format_type="dict",
                limit=limit,
                offset=offset,
                use_cache=use_cache,
            )
            return self._ensure_success(result)

        @app.get("/api/v1/stocks/{symbol}/snapshot", tags=["snapshot"])
        def get_stock_snapshot(
            symbol: str,
            trade_date: Optional[str] = Query(default=None, description="交易日期"),
            market: Optional[str] = Query(default=None, description="市场过滤"),
            fields: Optional[str] = Query(default=None, description="字段列表"),
            use_cache: bool = Query(default=True),
        ) -> Dict[str, Any]:
            field_list = self._parse_csv(fields)
            result = self.api_router.get_snapshot(
                symbols=symbol,
                trade_date=trade_date,
                market=market,
                fields=field_list,
                format_type="dict",
                use_cache=use_cache,
            )
            return self._ensure_success(result)

        @app.get(
            "/api/v1/snapshots",
            tags=["snapshot"],
            summary="获取多个股票快照",
        )
        def list_snapshots(
            symbols: Optional[str] = Query(
                default=None, description="逗号分隔的股票列表"
            ),
            trade_date: Optional[str] = Query(default=None),
            market: Optional[str] = Query(default=None),
            fields: Optional[str] = Query(default=None),
            limit: Optional[int] = Query(default=None, ge=1, le=5000),
            offset: int = Query(default=0, ge=0),
            use_cache: bool = Query(default=True),
        ) -> Dict[str, Any]:
            symbol_list = self._parse_csv(symbols)
            field_list = self._parse_csv(fields)
            result = self.api_router.get_snapshot(
                symbols=symbol_list,
                trade_date=trade_date,
                market=market,
                fields=field_list,
                format_type="dict",
                limit=limit,
                offset=offset,
                use_cache=use_cache,
            )
            return self._ensure_success(result)

        @app.get(
            "/api/v1/meta/stats",
            tags=["system"],
            summary="获取 API 运行状态",
        )
        def get_api_stats() -> Dict[str, Any]:
            return self.api_router.get_api_stats()

        # 兼容性的旧路由，标记为 deprecated
        @app.get(
            "/api/v1/stocks/{symbol}/price",
            tags=["deprecated"],
            deprecated=True,
        )
        def legacy_price_endpoint(
            symbol: str,
            start_date: Optional[str] = Query(default=None),
            end_date: Optional[str] = Query(default=None),
            frequency: str = Query(default="1d"),
        ) -> Dict[str, Any]:
            return get_stock_history(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
            )

        @app.exception_handler(Exception)
        async def unhandled_exception_handler(request, exc):  # type: ignore
            logger.exception("Unhandled exception in REST API", exc_info=exc)
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": "Internal server error",
                    "detail": str(exc),
                },
            )

    # ------------------------------------------------------------------
    # 管理方法
    # ------------------------------------------------------------------
    def start(self, threaded: bool = True) -> None:
        if self.is_running:
            logger.warning("REST API server already running")
            return

        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="debug" if self.debug else "info",
        )
        self._server = uvicorn.Server(config)

        if threaded:
            self.server_thread = threading.Thread(target=self._server.run, daemon=True)
            self.server_thread.start()
            self.is_running = True
            logger.info("REST API server started: http://%s:%s", self.host, self.port)
        else:
            self.is_running = True
            logger.info(
                "REST API server started (blocking): http://%s:%s",
                self.host,
                self.port,
            )
            try:
                self._server.run()
            finally:
                self.is_running = False

    def stop(self) -> None:
        if not self.is_running or not self._server:
            logger.warning("REST API server is not running")
            return

        self._server.should_exit = True
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)

        self.is_running = False
        self._server = None
        self.server_thread = None
        logger.info("REST API server stopped")

    def get_server_info(self) -> Dict[str, Any]:
        """Get server information including middleware stats"""
        info = {
            "server_name": "SimTradeData REST API",
            "version": "1.0.0",
            "host": self.host,
            "port": self.port,
            "is_running": self.is_running,
            "debug": self.debug,
            "enable_cors": self.enable_cors,
            "endpoints": [
                "GET /api/v1/health",
                "GET /api/v1/stocks",
                "GET /api/v1/stocks/{symbol}",
                "GET /api/v1/stocks/{symbol}/history",
                "GET /api/v1/stocks/{symbol}/fundamentals",
                "GET /api/v1/stocks/{symbol}/snapshot",
                "GET /api/v1/snapshots",
                "GET /api/v1/meta/stats",
            ],
        }

        # Add middleware statistics
        if self.request_logger:
            info["logging_stats"] = self.request_logger.get_stats()

        if self.rate_limiter:
            info["rate_limit_stats"] = self.rate_limiter.get_stats()

        if self.authenticator:
            info["authentication"] = {
                "enabled": self.enable_authentication,
                "registered_keys": len(self.api_keys),
            }

        return info

    def add_api_key(self, key: str, description: str = ""):
        """Add API key to authenticator"""
        if self.authenticator:
            self.authenticator.add_api_key(key, description)
        else:
            self.api_keys[key] = {
                "description": description,
                "created_at": datetime.now().isoformat(),
                "last_used": None,
            }
            logger.info("API key added to config: %s...", key[:8])

    def remove_api_key(self, key: str):
        """Remove API key from authenticator"""
        if self.authenticator:
            self.authenticator.remove_api_key(key)
        elif key in self.api_keys:
            del self.api_keys[key]
            logger.info("API key removed from config: %s...", key[:8])

    def cleanup_rate_limit_data(self):
        """Clean up expired rate limit data"""
        if self.rate_limiter:
            self.rate_limiter.cleanup_expired()

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_csv(value: Optional[str]) -> Optional[List[str]]:
        if value is None:
            return None
        items = [item.strip() for item in value.split(",") if item.strip()]
        return items if items else None

    @staticmethod
    def _ensure_success(result: Any) -> Dict[str, Any]:
        if isinstance(result, dict) and result.get("error"):
            detail = result.get("error_message") or "Unknown error"
            raise HTTPException(status_code=400, detail=detail)
        if isinstance(result, dict):
            return result
        raise HTTPException(status_code=500, detail="Unexpected response format")
