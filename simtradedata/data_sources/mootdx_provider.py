"""
Mootdx Data Provider

Wraps MootdxAdapter to conform to DataProvider interface.
Provides local and online data access through mootdx/TDX.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict

from .mootdx_adapter import MootdxAdapter
from .provider import (
    DataProvider,
    DataQuery,
    HealthStatus,
    ProviderCapabilities,
    ProviderError,
    ProviderStatus,
    ProviderType,
    QueryType,
)

logger = logging.getLogger(__name__)


class MootdxProvider(DataProvider):
    """
    Mootdx data provider implementation

    Wraps MootdxAdapter to provide plugin-compatible interface
    for accessing local TDX data and online market data.
    """

    def __init__(
        self,
        name: str = "mootdx",
        provider_type: ProviderType = ProviderType.LOCAL,
        priority: int = 1,
        enabled: bool = True,
        options: Dict[str, Any] = None,
    ):
        """
        Initialize Mootdx provider

        Args:
            name: Provider name
            provider_type: Provider type
            priority: Priority level
            enabled: Whether enabled
            options: Configuration options
        """
        super().__init__(name, provider_type, priority, enabled, options)

        # Initialize underlying adapter
        self._adapter = MootdxAdapter(config=options or {})

        # Health check cache
        self._last_health_check: datetime = None
        self._cached_health: HealthStatus = None
        self._health_check_interval = 300  # 5 minutes

        logger.info("MootdxProvider initialized with options: %s", options)

    def fetch(self, query: DataQuery) -> Dict[str, Any]:
        """
        Fetch data based on query

        Args:
            query: Data query object

        Returns:
            Dict containing query results

        Raises:
            ProviderError: If fetching fails
        """
        try:
            # Ensure adapter is connected
            if not self._adapter.is_connected():
                if not self._adapter.connect():
                    raise ProviderError("Failed to connect to mootdx")

            # Route query to appropriate method
            if query.query_type == QueryType.DAILY:
                return self._fetch_daily(query)
            elif query.query_type == QueryType.MINUTE:
                return self._fetch_minute(query)
            elif query.query_type == QueryType.STOCK_INFO:
                return self._fetch_stock_info(query)
            elif query.query_type == QueryType.FUNDAMENTALS:
                return self._fetch_fundamentals(query)
            else:
                raise ProviderError(f"Unsupported query type: {query.query_type.value}")

        except Exception as e:
            logger.error("Mootdx fetch failed: %s", e, exc_info=True)
            raise ProviderError(f"Fetch failed: {e}")

    def _fetch_daily(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch daily data"""
        if not query.symbol:
            raise ProviderError("Symbol required for daily data query")

        result = self._adapter.get_daily_data(
            symbol=query.symbol,
            start_date=query.start_date,
            end_date=query.end_date,
        )

        if not result.get("success"):
            raise ProviderError(result.get("error", "Unknown error"))

        return result

    def _fetch_minute(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch minute data"""
        if not query.symbol or not query.start_date:
            raise ProviderError("Symbol and start_date required for minute data")

        frequency = query.frequency or "5m"

        result = self._adapter.get_minute_data(
            symbol=query.symbol,
            trade_date=query.start_date,
            frequency=frequency,
        )

        if not result.get("success"):
            raise ProviderError(result.get("error", "Unknown error"))

        return result

    def _fetch_stock_info(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch stock info"""
        result = self._adapter.get_stock_info(symbol=query.symbol)

        if isinstance(result, dict) and not result.get("success"):
            raise ProviderError(result.get("error", "Unknown error"))

        return result if isinstance(result, dict) else {"success": True, "data": result}

    def _fetch_fundamentals(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch fundamentals data"""
        if not query.symbol or not query.start_date:
            raise ProviderError("Symbol and report_date required for fundamentals")

        report_type = query.extra_params.get("report_type", "Q4")

        result = self._adapter.get_fundamentals(
            symbol=query.symbol,
            report_date=query.start_date,
            report_type=report_type,
        )

        if not result.get("success"):
            raise ProviderError(result.get("error", "Unknown error"))

        return result

    def health(self) -> HealthStatus:
        """
        Check provider health status

        Returns:
            HealthStatus object
        """
        # Check cache
        if self._cached_health and self._last_health_check:
            elapsed = (datetime.now() - self._last_health_check).total_seconds()
            if elapsed < self._health_check_interval:
                return self._cached_health

        # Perform health check
        start_time = time.time()
        status = ProviderStatus.HEALTHY
        error_message = None

        try:
            # Try to connect
            if not self._adapter.is_connected():
                connected = self._adapter.connect()
                if not connected:
                    status = ProviderStatus.UNHEALTHY
                    error_message = "Failed to connect to mootdx"
            else:
                # Already connected, just verify
                status = ProviderStatus.HEALTHY

        except Exception as e:
            status = ProviderStatus.UNHEALTHY
            error_message = str(e)
            logger.warning("Mootdx health check failed: %s", e)

        response_time = time.time() - start_time

        # Create and cache health status
        health_status = HealthStatus(
            status=status,
            last_check=datetime.now(),
            response_time=response_time,
            error_message=error_message,
        )

        self._cached_health = health_status
        self._last_health_check = datetime.now()

        return health_status

    def capabilities(self) -> ProviderCapabilities:
        """
        Get provider capabilities

        Returns:
            ProviderCapabilities object
        """
        # Get capabilities from adapter if available
        adapter_caps = self._adapter.get_capabilities()

        return ProviderCapabilities(
            name=self.name,
            supported_queries=[
                QueryType.DAILY,
                QueryType.MINUTE,
                QueryType.STOCK_INFO,
                QueryType.FUNDAMENTALS,
            ],
            supported_frequencies=adapter_caps.get(
                "supported_frequencies", ["1d", "5m", "15m", "30m", "60m"]
            ),
            supported_markets=adapter_caps.get("supported_markets", ["SZ", "SS"]),
            rate_limit=adapter_caps.get("rate_limit", 100),
            timeout=adapter_caps.get("timeout", 10),
            requires_auth=False,
            data_source="mootdx/TDX",
            connection_type="local+online",
        )

    def __del__(self):
        """Cleanup on deletion"""
        try:
            if self._adapter:
                self._adapter.disconnect()
        except Exception:
            pass
