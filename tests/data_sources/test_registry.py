"""
Tests for Data Source Registry and Provider System

Tests provider interface, registry functionality, and plugin loading.
"""

from datetime import datetime

import pytest

from simtradedata.data_sources.provider import (
    DataProvider,
    DataQuery,
    HealthStatus,
    ProviderCapabilities,
    ProviderError,
    ProviderStatus,
    ProviderType,
    QueryType,
)
from simtradedata.data_sources.registry import DataSourceRegistry


# Mock Provider Implementation
class MockProvider(DataProvider):
    """Mock provider for testing"""

    def __init__(self, name, provider_type, priority=0, enabled=True, options=None):
        super().__init__(name, provider_type, priority, enabled, options)
        self._fetch_called = False
        self._health_called = False
        self._capabilities_called = False
        self._should_fail = False

    def fetch(self, query: DataQuery):
        """Mock fetch implementation"""
        self._fetch_called = True
        if self._should_fail:
            raise ProviderError("Simulated fetch failure")
        return {
            "success": True,
            "data": [{"symbol": query.symbol, "value": 100}],
            "provider": self.name,
        }

    def health(self) -> HealthStatus:
        """Mock health check"""
        self._health_called = True
        if self._should_fail:
            return HealthStatus(
                status=ProviderStatus.UNHEALTHY,
                last_check=datetime.now(),
                error_message="Simulated failure",
            )
        return HealthStatus(
            status=ProviderStatus.HEALTHY,
            last_check=datetime.now(),
            response_time=0.01,
        )

    def capabilities(self) -> ProviderCapabilities:
        """Mock capabilities"""
        self._capabilities_called = True
        return ProviderCapabilities(
            name=self.name,
            supported_queries=[QueryType.DAILY, QueryType.MINUTE],
            supported_frequencies=["1d", "5m"],
            supported_markets=["SZ", "SS"],
            rate_limit=100,
            timeout=10,
        )


class TestDataProvider:
    """Test DataProvider interface"""

    def test_provider_initialization(self):
        """Test provider creation"""
        provider = MockProvider(
            name="test_provider",
            provider_type=ProviderType.LOCAL,
            priority=1,
            enabled=True,
        )

        assert provider.name == "test_provider"
        assert provider.provider_type == ProviderType.LOCAL
        assert provider.priority == 1
        assert provider.enabled is True

    def test_provider_fetch(self):
        """Test provider fetch method"""
        provider = MockProvider("test", ProviderType.ONLINE, priority=1)

        query = DataQuery(
            query_type=QueryType.DAILY,
            symbol="000001.SZ",
            start_date="2024-01-01",
        )

        result = provider.fetch(query)

        assert provider._fetch_called
        assert result["success"] is True
        assert "data" in result

    def test_provider_health(self):
        """Test provider health check"""
        provider = MockProvider("test", ProviderType.ONLINE)

        health = provider.health()

        assert provider._health_called
        assert health.status == ProviderStatus.HEALTHY
        assert health.response_time is not None

    def test_provider_capabilities(self):
        """Test provider capabilities"""
        provider = MockProvider("test", ProviderType.LOCAL)

        caps = provider.capabilities()

        assert provider._capabilities_called
        assert caps.name == "test"
        assert QueryType.DAILY in caps.supported_queries
        assert "1d" in caps.supported_frequencies

    def test_provider_is_available(self):
        """Test provider availability check"""
        provider = MockProvider("test", ProviderType.ONLINE, enabled=True)
        assert provider.is_available() is True

        # Disabled provider
        provider.enabled = False
        assert provider.is_available() is False

    def test_provider_supports_query(self):
        """Test query support check"""
        provider = MockProvider("test", ProviderType.ONLINE)

        query_daily = DataQuery(query_type=QueryType.DAILY)
        assert provider.supports_query(query_daily) is True

        query_fundamentals = DataQuery(query_type=QueryType.FUNDAMENTALS)
        assert provider.supports_query(query_fundamentals) is False


class TestDataSourceRegistry:
    """Test DataSourceRegistry"""

    @pytest.fixture
    def sample_config(self):
        """Create sample configuration"""
        return {
            "data_sources": [
                {
                    "name": "provider1",
                    "module": "test_module.Provider1",
                    "priority": 1,
                    "type": "local",
                    "enabled": True,
                    "options": {"key1": "value1"},
                },
                {
                    "name": "provider2",
                    "module": "test_module.Provider2",
                    "priority": 2,
                    "type": "online",
                    "enabled": True,
                    "options": {"key2": "value2"},
                },
                {
                    "name": "provider3",
                    "module": "test_module.Provider3",
                    "priority": 3,
                    "type": "online",
                    "enabled": False,
                    "options": {},
                },
            ]
        }

    def test_registry_initialization_with_dict(self):
        """Test registry initialization with config dict"""
        config = {"data_sources": []}
        registry = DataSourceRegistry(config_dict=config)

        assert registry is not None
        assert len(registry.list_providers()) == 0

    def test_registry_with_mock_providers(self):
        """Test registry with manually added providers"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        # Manually add mock providers
        provider1 = MockProvider("test1", ProviderType.LOCAL, priority=1)
        provider2 = MockProvider("test2", ProviderType.ONLINE, priority=2)

        registry._providers["test1"] = provider1
        registry._providers["test2"] = provider2

        assert len(registry.list_providers()) == 2

    def test_registry_list_providers(self):
        """Test listing providers"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider1 = MockProvider("test1", ProviderType.LOCAL, priority=2, enabled=True)
        provider2 = MockProvider("test2", ProviderType.ONLINE, priority=1, enabled=True)
        provider3 = MockProvider(
            "test3", ProviderType.ONLINE, priority=3, enabled=False
        )

        registry._providers["test1"] = provider1
        registry._providers["test2"] = provider2
        registry._providers["test3"] = provider3

        # All providers
        all_providers = registry.list_providers()
        assert len(all_providers) == 3
        # Should be sorted by priority
        assert all_providers[0].name == "test2"  # priority 1

        # Enabled only
        enabled = registry.list_providers(enabled_only=True)
        assert len(enabled) == 2

        # Available only (enabled + healthy)
        available = registry.list_providers(available_only=True)
        assert len(available) == 2

    def test_registry_get_provider(self):
        """Test getting provider by name"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider = MockProvider("test", ProviderType.LOCAL)
        registry._providers["test"] = provider

        found = registry.get_provider("test")
        assert found is not None
        assert found.name == "test"

        not_found = registry.get_provider("nonexistent")
        assert not_found is None

    def test_registry_next_available(self):
        """Test getting next available provider"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider1 = MockProvider("test1", ProviderType.LOCAL, priority=2, enabled=True)
        provider2 = MockProvider("test2", ProviderType.ONLINE, priority=1, enabled=True)

        registry._providers["test1"] = provider1
        registry._providers["test2"] = provider2

        # Should get provider with lowest priority number
        next_provider = registry.next_available()
        assert next_provider is not None
        assert next_provider.name == "test2"  # priority 1

        # Exclude provider
        next_provider = registry.next_available(exclude=["test2"])
        assert next_provider is not None
        assert next_provider.name == "test1"

    def test_registry_next_available_by_query_type(self):
        """Test filtering providers by query type"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider = MockProvider("test", ProviderType.LOCAL, priority=1, enabled=True)
        registry._providers["test"] = provider

        # Query type supported
        next_provider = registry.next_available(query_type=QueryType.DAILY)
        assert next_provider is not None

        # Query type not supported
        next_provider = registry.next_available(query_type=QueryType.FUNDAMENTALS)
        assert next_provider is None

    def test_registry_health_check_all(self):
        """Test health check for all providers"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider1 = MockProvider("test1", ProviderType.LOCAL, priority=1)
        provider2 = MockProvider("test2", ProviderType.ONLINE, priority=2)

        registry._providers["test1"] = provider1
        registry._providers["test2"] = provider2

        health_results = registry.health_check_all(force=True)

        assert len(health_results) == 2
        assert "test1" in health_results
        assert "test2" in health_results
        assert health_results["test1"].status == ProviderStatus.HEALTHY

    def test_registry_get_stats(self):
        """Test getting registry statistics"""
        registry = DataSourceRegistry(config_dict={"data_sources": []})

        provider = MockProvider("test", ProviderType.LOCAL, priority=1)
        registry._providers["test"] = provider
        registry._stats["total_providers"] = 1
        registry._stats["enabled_providers"] = 1

        stats = registry.get_stats()

        assert stats["provider_count"] == 1
        assert stats["total_providers"] == 1
        assert "health_check_interval" in stats


class TestProviderHelpers:
    """Test provider helper classes"""

    def test_data_query_creation(self):
        """Test DataQuery creation"""
        query = DataQuery(
            query_type=QueryType.DAILY,
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-31",
            frequency="1d",
        )

        assert query.query_type == QueryType.DAILY
        assert query.symbol == "000001.SZ"

        query_dict = query.to_dict()
        assert query_dict["query_type"] == "daily"

    def test_provider_capabilities_creation(self):
        """Test ProviderCapabilities creation"""
        caps = ProviderCapabilities(
            name="test",
            supported_queries=[QueryType.DAILY],
            supported_frequencies=["1d"],
            supported_markets=["SZ"],
        )

        assert caps.supports(QueryType.DAILY) is True
        assert caps.supports(QueryType.MINUTE) is False

        caps_dict = caps.to_dict()
        assert "supported_queries" in caps_dict

    def test_health_status_creation(self):
        """Test HealthStatus creation"""
        health = HealthStatus(
            status=ProviderStatus.HEALTHY,
            last_check=datetime.now(),
            response_time=0.05,
        )

        assert health.status == ProviderStatus.HEALTHY
        assert health.response_time == 0.05

        health_dict = health.to_dict()
        assert health_dict["status"] == "healthy"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
