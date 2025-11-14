"""
Data Source Provider Protocol

Defines the interface that all data source providers must implement
for plugin-based architecture.
"""

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class ProviderStatus(str, Enum):
    """Provider health status"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ProviderType(str, Enum):
    """Provider type classification"""

    LOCAL = "local"  # Local data source (e.g., mootdx)
    ONLINE = "online"  # Online API (e.g., qstock, baostock)
    DATABASE = "database"  # Database connection
    FILE = "file"  # File-based source


class QueryType(str, Enum):
    """Supported query types"""

    DAILY = "daily"
    MINUTE = "minute"
    STOCK_INFO = "stock_info"
    FUNDAMENTALS = "fundamentals"
    TRADE_CALENDAR = "trade_calendar"
    ADJUSTMENT = "adjustment"
    VALUATION = "valuation"


class DataQuery:
    """Standard query object for data fetching"""

    def __init__(
        self,
        query_type: QueryType,
        symbol: Optional[str] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        frequency: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ):
        self.query_type = query_type
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.fields = fields
        self.extra_params = kwargs

    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary"""
        return {
            "query_type": self.query_type.value,
            "symbol": self.symbol,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None,
            "frequency": self.frequency,
            "fields": self.fields,
            **self.extra_params,
        }


class ProviderCapabilities:
    """Provider capabilities description"""

    def __init__(
        self,
        name: str,
        supported_queries: List[QueryType],
        supported_frequencies: List[str],
        supported_markets: List[str],
        rate_limit: int = 100,
        timeout: int = 10,
        requires_auth: bool = False,
        **metadata,
    ):
        self.name = name
        self.supported_queries = supported_queries
        self.supported_frequencies = supported_frequencies
        self.supported_markets = supported_markets
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.requires_auth = requires_auth
        self.metadata = metadata

    def supports(self, query_type: QueryType) -> bool:
        """Check if provider supports a query type"""
        return query_type in self.supported_queries

    def to_dict(self) -> Dict[str, Any]:
        """Convert capabilities to dictionary"""
        return {
            "name": self.name,
            "supported_queries": [q.value for q in self.supported_queries],
            "supported_frequencies": self.supported_frequencies,
            "supported_markets": self.supported_markets,
            "rate_limit": self.rate_limit,
            "timeout": self.timeout,
            "requires_auth": self.requires_auth,
            **self.metadata,
        }


class HealthStatus:
    """Provider health status information"""

    def __init__(
        self,
        status: ProviderStatus,
        last_check: datetime,
        response_time: Optional[float] = None,
        error_message: Optional[str] = None,
        consecutive_failures: int = 0,
    ):
        self.status = status
        self.last_check = last_check
        self.response_time = response_time
        self.error_message = error_message
        self.consecutive_failures = consecutive_failures

    def to_dict(self) -> Dict[str, Any]:
        """Convert health status to dictionary"""
        return {
            "status": self.status.value,
            "last_check": self.last_check.isoformat(),
            "response_time": self.response_time,
            "error_message": self.error_message,
            "consecutive_failures": self.consecutive_failures,
        }


class DataProvider(ABC):
    """
    Abstract base class for data source providers

    All data source providers must implement this interface to be
    compatible with the DataSourceRegistry plugin system.
    """

    def __init__(
        self,
        name: str,
        provider_type: ProviderType,
        priority: int = 0,
        enabled: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize data provider

        Args:
            name: Provider name
            provider_type: Provider type
            priority: Priority level (lower = higher priority)
            enabled: Whether provider is enabled
            options: Provider-specific configuration options
        """
        self.name = name
        self.provider_type = provider_type
        self.priority = priority
        self.enabled = enabled
        self.options = options or {}

        self._health_status: Optional[HealthStatus] = None
        self._capabilities: Optional[ProviderCapabilities] = None

        logger.info(
            "Provider %s initialized (type=%s, priority=%d)",
            name,
            provider_type.value,
            priority,
        )

    @abstractmethod
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

    @abstractmethod
    def health(self) -> HealthStatus:
        """
        Check provider health status

        Returns:
            HealthStatus object

        This method should perform a lightweight check (e.g., ping)
        and return quickly. Results should be cached internally.
        """

    @abstractmethod
    def capabilities(self) -> ProviderCapabilities:
        """
        Get provider capabilities

        Returns:
            ProviderCapabilities object describing what this provider supports
        """

    def is_available(self) -> bool:
        """
        Check if provider is available for use

        Returns:
            True if provider is enabled and healthy
        """
        if not self.enabled:
            return False

        health_status = self.health()
        return health_status.status in (ProviderStatus.HEALTHY, ProviderStatus.DEGRADED)

    def supports_query(self, query: DataQuery) -> bool:
        """
        Check if provider supports a specific query

        Args:
            query: Query to check

        Returns:
            True if query is supported
        """
        caps = self.capabilities()
        return caps.supports(query.query_type)

    def __str__(self):
        return f"DataProvider({self.name}, type={self.provider_type.value}, priority={self.priority})"

    def __repr__(self):
        return self.__str__()


class ProviderError(Exception):
    """Base exception for provider errors"""


class ProviderConnectionError(ProviderError):
    """Provider connection error"""


class ProviderDataError(ProviderError):
    """Provider data error"""


class ProviderConfigError(ProviderError):
    """Provider configuration error"""
