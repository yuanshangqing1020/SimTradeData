"""
FastAPI Middleware

Provides rate limiting, authentication, logging and other gateway features
as FastAPI middleware components.
"""

import logging
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Callable, Dict, Optional

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware for FastAPI

    Controls request rate per client based on IP address
    """

    def __init__(
        self,
        app,
        enabled: bool = True,
        max_requests: int = 1000,
        window_seconds: int = 3600,
    ):
        """
        Initialize rate limiter

        Args:
            app: FastAPI application instance
            enabled: Enable/disable rate limiting
            max_requests: Maximum requests allowed per window
            window_seconds: Time window in seconds
        """
        super().__init__(app)
        self.enabled = enabled
        self.max_requests = max_requests
        self.window_seconds = window_seconds

        # Client request tracking: {client_id: deque of timestamps}
        self.request_counts: Dict[str, deque] = defaultdict(deque)

        # Statistics
        self.stats = {
            "total_requests": 0,
            "rate_limited_requests": 0,
        }

        logger.info(
            "Rate limit middleware initialized: max_requests=%d, window=%ds",
            max_requests,
            window_seconds,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with rate limiting"""

        if not self.enabled:
            return await call_next(request)

        # Extract client identifier (IP address)
        client_id = self._get_client_id(request)

        # Update statistics
        self.stats["total_requests"] += 1

        # Check rate limit
        if not self._is_request_allowed(client_id):
            self.stats["rate_limited_requests"] += 1
            logger.warning("Rate limit exceeded for client: %s", client_id)

            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "success": False,
                    "error": "Rate limit exceeded",
                    "error_code": "RATE_LIMITED",
                    "retry_after": self.window_seconds,
                },
            )

        # Process request
        response = await call_next(request)
        return response

    def _get_client_id(self, request: Request) -> str:
        """Extract client identifier from request"""
        # Check for forwarded IP (if behind proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Use client IP
        if request.client:
            return request.client.host

        # Fallback
        return "unknown"

    def _is_request_allowed(self, client_id: str) -> bool:
        """Check if client request is within rate limit"""
        now = time.time()
        window_start = now - self.window_seconds

        # Get client request history
        requests = self.request_counts[client_id]

        # Remove expired records
        while requests and requests[0] < window_start:
            requests.popleft()

        # Check if limit exceeded
        if len(requests) >= self.max_requests:
            return False

        # Record current request
        requests.append(now)
        return True

    def get_stats(self) -> Dict:
        """Get rate limiter statistics"""
        return {
            "enabled": self.enabled,
            "max_requests": self.max_requests,
            "window_seconds": self.window_seconds,
            "active_clients": len(self.request_counts),
            "total_requests": self.stats["total_requests"],
            "rate_limited_requests": self.stats["rate_limited_requests"],
        }

    def cleanup_expired(self):
        """Remove expired client records"""
        now = time.time()
        window_start = now - self.window_seconds

        clients_to_remove = []
        for client_id, requests in self.request_counts.items():
            # Remove expired records
            while requests and requests[0] < window_start:
                requests.popleft()

            # Mark client for removal if no active requests
            if not requests:
                clients_to_remove.append(client_id)

        # Clean up inactive clients
        for client_id in clients_to_remove:
            del self.request_counts[client_id]

        if clients_to_remove:
            logger.debug("Cleaned up %d inactive clients", len(clients_to_remove))


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """
    Authentication middleware for FastAPI

    Validates API keys for protected endpoints
    """

    def __init__(
        self,
        app,
        enabled: bool = False,
        api_keys: Optional[Dict[str, Dict]] = None,
        public_paths: Optional[list] = None,
    ):
        """
        Initialize authentication middleware

        Args:
            app: FastAPI application instance
            enabled: Enable/disable authentication
            api_keys: Dictionary of valid API keys and metadata
            public_paths: List of paths that don't require authentication
        """
        super().__init__(app)
        self.enabled = enabled
        self.api_keys = api_keys or {}
        self.public_paths = public_paths or [
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/v1/health",
        ]

        logger.info(
            "Authentication middleware initialized: enabled=%s, keys=%d",
            enabled,
            len(self.api_keys),
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request with authentication"""

        if not self.enabled:
            return await call_next(request)

        # Check if path is public
        if self._is_public_path(request.url.path):
            return await call_next(request)

        # Extract API key from header
        api_key = request.headers.get("X-API-Key") or request.headers.get(
            "Authorization"
        )

        # Validate API key
        if not self._validate_api_key(api_key):
            logger.warning(
                "Authentication failed for path: %s from client: %s",
                request.url.path,
                self._get_client_id(request),
            )

            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "error": "Authentication failed",
                    "error_code": "AUTH_FAILED",
                },
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Update last used timestamp
        if api_key in self.api_keys:
            self.api_keys[api_key]["last_used"] = datetime.now().isoformat()

        # Process request
        response = await call_next(request)
        return response

    def _is_public_path(self, path: str) -> bool:
        """Check if path is public (no auth required)"""
        return any(path.startswith(public_path) for public_path in self.public_paths)

    def _validate_api_key(self, api_key: Optional[str]) -> bool:
        """Validate API key"""
        if not api_key:
            return False

        # Remove "Bearer " prefix if present
        if api_key.startswith("Bearer "):
            api_key = api_key[7:]

        return api_key in self.api_keys

    def _get_client_id(self, request: Request) -> str:
        """Extract client identifier"""
        if request.client:
            return request.client.host
        return "unknown"

    def add_api_key(self, key: str, description: str = ""):
        """Add new API key"""
        self.api_keys[key] = {
            "description": description,
            "created_at": datetime.now().isoformat(),
            "last_used": None,
        }
        logger.info("API key added: %s...", key[:8])

    def remove_api_key(self, key: str):
        """Remove API key"""
        if key in self.api_keys:
            del self.api_keys[key]
            logger.info("API key removed: %s...", key[:8])


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Request logging middleware for FastAPI

    Logs all API requests with timing and status information
    """

    def __init__(
        self,
        app,
        enabled: bool = True,
        log_request_body: bool = False,
        log_response_body: bool = False,
    ):
        """
        Initialize logging middleware

        Args:
            app: FastAPI application instance
            enabled: Enable/disable request logging
            log_request_body: Log request body content
            log_response_body: Log response body content
        """
        super().__init__(app)
        self.enabled = enabled
        self.log_request_body = log_request_body
        self.log_response_body = log_response_body

        # Statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "start_time": datetime.now(),
        }

        logger.info("Request logging middleware initialized")

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process and log request"""

        if not self.enabled:
            return await call_next(request)

        # Record request start time
        start_time = time.time()

        # Extract client info
        client_id = self._get_client_id(request)

        # Update statistics
        self.stats["total_requests"] += 1

        # Process request
        try:
            response = await call_next(request)

            # Calculate response time
            response_time = time.time() - start_time

            # Update success statistics
            if 200 <= response.status_code < 400:
                self.stats["successful_requests"] += 1
                status_label = "success"
            else:
                self.stats["failed_requests"] += 1
                status_label = "error"

            # Log request
            self._log_request(
                client_id=client_id,
                method=request.method,
                path=request.url.path,
                query=str(request.url.query) if request.url.query else None,
                status_code=response.status_code,
                response_time=response_time,
                status=status_label,
            )

            return response

        except Exception as e:
            # Calculate response time
            response_time = time.time() - start_time

            # Update failure statistics
            self.stats["failed_requests"] += 1

            # Log error
            logger.error(
                "Request failed: %s %s - %s (%.3fs)",
                request.method,
                request.url.path,
                str(e),
                response_time,
                exc_info=True,
            )

            # Re-raise exception
            raise

    def _get_client_id(self, request: Request) -> str:
        """Extract client identifier"""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        if request.client:
            return request.client.host

        return "unknown"

    def _log_request(
        self,
        client_id: str,
        method: str,
        path: str,
        query: Optional[str],
        status_code: int,
        response_time: float,
        status: str,
    ):
        """Log request details"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "client_id": client_id,
            "method": method,
            "path": path,
            "query": query,
            "status_code": status_code,
            "response_time": f"{response_time:.3f}s",
            "status": status,
        }

        logger.info(
            "API request: %s %s - %d (%.3fs) [%s]",
            method,
            path,
            status_code,
            response_time,
            client_id,
        )

    def get_stats(self) -> Dict:
        """Get logging statistics"""
        uptime = datetime.now() - self.stats["start_time"]
        return {
            "enabled": self.enabled,
            "total_requests": self.stats["total_requests"],
            "successful_requests": self.stats["successful_requests"],
            "failed_requests": self.stats["failed_requests"],
            "uptime_seconds": uptime.total_seconds(),
        }
