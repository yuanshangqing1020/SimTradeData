"""
Qstock and Baostock Data Providers

Simplified provider wrappers for qstock and baostock data sources.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict

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


class QstockProvider(DataProvider):
    """
    Qstock data provider implementation

    Provides online market data through qstock API.
    """

    def __init__(
        self,
        name: str = "qstock",
        provider_type: ProviderType = ProviderType.ONLINE,
        priority: int = 2,
        enabled: bool = True,
        options: Dict[str, Any] = None,
    ):
        super().__init__(name, provider_type, priority, enabled, options)

        # Lazy load adapter
        self._adapter = None
        self._last_health_check = None
        self._cached_health = None

    def _get_adapter(self):
        """Lazy load qstock adapter"""
        if self._adapter is None:
            from .qstock_adapter import QStockAdapter

            self._adapter = QStockAdapter(config=self.options or {})
        return self._adapter

    def fetch(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch data based on query"""
        try:
            adapter = self._get_adapter()

            if not adapter.is_connected():
                if not adapter.connect():
                    raise ProviderError("Failed to connect to qstock")

            # Route to appropriate method
            if query.query_type == QueryType.DAILY:
                result = adapter.get_daily_data(
                    symbol=query.symbol,
                    start_date=query.start_date,
                    end_date=query.end_date,
                )
            elif query.query_type == QueryType.MINUTE:
                result = adapter.get_minute_data(
                    symbol=query.symbol,
                    trade_date=query.start_date,
                    frequency=query.frequency or "5m",
                )
            elif query.query_type == QueryType.STOCK_INFO:
                result = adapter.get_stock_info(symbol=query.symbol)
            elif query.query_type == QueryType.FUNDAMENTALS:
                result = adapter.get_fundamentals(
                    symbol=query.symbol,
                    report_date=query.start_date,
                    report_type=query.extra_params.get("report_type", "Q4"),
                )
            else:
                raise ProviderError(f"Unsupported query type: {query.query_type.value}")

            # Convert result to standard format if needed
            if isinstance(result, dict):
                return result
            else:
                return {"success": True, "data": result}

        except Exception as e:
            logger.error("Qstock fetch failed: %s", e, exc_info=True)
            raise ProviderError(f"Fetch failed: {e}")

    def health(self) -> HealthStatus:
        """Check provider health"""
        # Simple health check
        start_time = time.time()
        status = ProviderStatus.HEALTHY
        error_message = None

        try:
            adapter = self._get_adapter()
            if not adapter.is_connected():
                connected = adapter.connect()
                if not connected:
                    status = ProviderStatus.UNHEALTHY
                    error_message = "Failed to connect"
        except Exception as e:
            status = ProviderStatus.UNHEALTHY
            error_message = str(e)

        response_time = time.time() - start_time

        return HealthStatus(
            status=status,
            last_check=datetime.now(),
            response_time=response_time,
            error_message=error_message,
        )

    def capabilities(self) -> ProviderCapabilities:
        """Get provider capabilities"""
        return ProviderCapabilities(
            name=self.name,
            supported_queries=[
                QueryType.DAILY,
                QueryType.MINUTE,
                QueryType.STOCK_INFO,
                QueryType.FUNDAMENTALS,
            ],
            supported_frequencies=["1m", "5m", "15m", "30m", "60m", "1d"],
            supported_markets=["SZ", "SS", "HK", "US"],
            rate_limit=200,
            timeout=15,
            requires_auth=False,
            data_source="qstock",
        )


class BaostockProvider(DataProvider):
    """
    Baostock data provider implementation

    Provides online market data through baostock API.
    """

    def __init__(
        self,
        name: str = "baostock",
        provider_type: ProviderType = ProviderType.ONLINE,
        priority: int = 3,
        enabled: bool = True,
        options: Dict[str, Any] = None,
    ):
        super().__init__(name, provider_type, priority, enabled, options)

        # Lazy load adapter
        self._adapter = None
        self._last_health_check = None
        self._cached_health = None

    def _get_adapter(self):
        """Lazy load baostock adapter"""
        if self._adapter is None:
            from .baostock_adapter import BaoStockAdapter

            self._adapter = BaoStockAdapter(config=self.options or {})
        return self._adapter

    def fetch(self, query: DataQuery) -> Dict[str, Any]:
        """Fetch data based on query"""
        try:
            adapter = self._get_adapter()

            if not adapter.is_connected():
                if not adapter.connect():
                    raise ProviderError("Failed to connect to baostock")

            # Route to appropriate method
            if query.query_type == QueryType.DAILY:
                result = adapter.get_daily_data(
                    symbol=query.symbol,
                    start_date=query.start_date,
                    end_date=query.end_date,
                )
            elif query.query_type == QueryType.MINUTE:
                result = adapter.get_minute_data(
                    symbol=query.symbol,
                    trade_date=query.start_date,
                    frequency=query.frequency or "5m",
                )
            elif query.query_type == QueryType.STOCK_INFO:
                result = adapter.get_stock_info(symbol=query.symbol)
            elif query.query_type == QueryType.FUNDAMENTALS:
                result = adapter.get_fundamentals(
                    symbol=query.symbol,
                    report_date=query.start_date,
                    report_type=query.extra_params.get("report_type", "Q4"),
                )
            else:
                raise ProviderError(f"Unsupported query type: {query.query_type.value}")

            # Convert result to standard format if needed
            if isinstance(result, dict):
                return result
            else:
                return {"success": True, "data": result}

        except Exception as e:
            logger.error("Baostock fetch failed: %s", e, exc_info=True)
            raise ProviderError(f"Fetch failed: {e}")

    def health(self) -> HealthStatus:
        """Check provider health"""
        start_time = time.time()
        status = ProviderStatus.HEALTHY
        error_message = None

        try:
            adapter = self._get_adapter()
            if not adapter.is_connected():
                connected = adapter.connect()
                if not connected:
                    status = ProviderStatus.UNHEALTHY
                    error_message = "Failed to connect"
        except Exception as e:
            status = ProviderStatus.UNHEALTHY
            error_message = str(e)

        response_time = time.time() - start_time

        return HealthStatus(
            status=status,
            last_check=datetime.now(),
            response_time=response_time,
            error_message=error_message,
        )

    def capabilities(self) -> ProviderCapabilities:
        """Get provider capabilities"""
        return ProviderCapabilities(
            name=self.name,
            supported_queries=[
                QueryType.DAILY,
                QueryType.MINUTE,
                QueryType.STOCK_INFO,
                QueryType.FUNDAMENTALS,
            ],
            supported_frequencies=["5m", "15m", "30m", "60m", "1d"],
            supported_markets=["SZ", "SS"],
            rate_limit=150,
            timeout=20,
            requires_auth=False,
            data_source="baostock",
        )
