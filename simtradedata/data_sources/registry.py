"""
Data Source Registry

Manages data source providers through configuration-based plugin system.
Supports dynamic loading, priority-based selection, and health monitoring.
"""

import importlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .provider import (
    DataProvider,
    HealthStatus,
    ProviderConfigError,
    ProviderStatus,
    ProviderType,
    QueryType,
)

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    """
    Central registry for data source providers

    Loads providers from configuration files, manages their lifecycle,
    and provides selection logic based on priority and health status.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config_dict: Optional[Dict] = None,
        health_check_interval: int = 300,  # 5 minutes
    ):
        """
        Initialize registry

        Args:
            config_path: Path to configuration file (YAML or JSON)
            config_dict: Configuration dictionary (alternative to file)
            health_check_interval: Seconds between health checks
        """
        self.config_path = config_path
        self.config_dict = config_dict
        self.health_check_interval = health_check_interval

        # Provider storage
        self._providers: Dict[str, DataProvider] = {}
        self._health_cache: Dict[str, HealthStatus] = {}
        self._last_health_check: Dict[str, datetime] = {}

        # Statistics
        self._stats = {
            "total_providers": 0,
            "enabled_providers": 0,
            "healthy_providers": 0,
            "degraded_providers": 0,
            "unhealthy_providers": 0,
            "total_queries": 0,
            "failed_queries": 0,
        }

        # Load configuration
        if config_path or config_dict:
            self._load_configuration()

        logger.info(
            "DataSourceRegistry initialized with %d providers",
            len(self._providers),
        )

    def _load_configuration(self):
        """Load configuration from file or dict"""
        try:
            if self.config_dict:
                config = self.config_dict
            elif self.config_path:
                config = self._read_config_file(self.config_path)
            else:
                raise ProviderConfigError("No configuration source provided")

            # Parse and load providers
            providers_config = config.get("data_sources", [])
            for provider_config in providers_config:
                self._load_provider(provider_config)

            logger.info("Configuration loaded successfully")

        except Exception as e:
            logger.error("Failed to load configuration: %s", e, exc_info=True)
            raise ProviderConfigError(f"Configuration loading failed: {e}")

    def _read_config_file(self, file_path: str) -> Dict:
        """Read configuration from YAML or JSON file"""
        path = Path(file_path)

        if not path.exists():
            raise ProviderConfigError(f"Configuration file not found: {file_path}")

        with open(path, "r", encoding="utf-8") as f:
            if path.suffix in (".yaml", ".yml"):
                return yaml.safe_load(f)
            elif path.suffix == ".json":
                return json.load(f)
            else:
                raise ProviderConfigError(f"Unsupported config format: {path.suffix}")

    def _load_provider(self, config: Dict[str, Any]):
        """
        Load a single provider from configuration

        Args:
            config: Provider configuration dict
        """
        try:
            name = config.get("name")
            module_path = config.get("module")
            priority = config.get("priority", 99)
            enabled = config.get("enabled", True)
            provider_type_str = config.get("type", "online")
            options = config.get("options", {})

            if not name or not module_path:
                logger.warning("Provider config missing name or module: %s", config)
                return

            # Convert type string to enum
            try:
                provider_type = ProviderType(provider_type_str.lower())
            except ValueError:
                logger.warning(
                    "Invalid provider type '%s', defaulting to online",
                    provider_type_str,
                )
                provider_type = ProviderType.ONLINE

            # Dynamic import
            try:
                module_parts = module_path.rsplit(".", 1)
                if len(module_parts) == 2:
                    module_name, class_name = module_parts
                else:
                    # Assume class name is module name capitalized
                    module_name = module_path
                    class_name = name.capitalize() + "Provider"

                module = importlib.import_module(module_name)
                provider_class = getattr(module, class_name)

                # Instantiate provider
                provider = provider_class(
                    name=name,
                    provider_type=provider_type,
                    priority=priority,
                    enabled=enabled,
                    options=options,
                )

                # Register provider
                self._providers[name] = provider
                self._stats["total_providers"] += 1
                if enabled:
                    self._stats["enabled_providers"] += 1

                logger.info(
                    "Provider '%s' loaded successfully (priority=%d, enabled=%s)",
                    name,
                    priority,
                    enabled,
                )

            except (ImportError, AttributeError) as e:
                logger.error(
                    "Failed to import provider '%s' from '%s': %s",
                    name,
                    module_path,
                    e,
                )
                raise ProviderConfigError(f"Provider import failed for '{name}': {e}")

        except Exception as e:
            logger.error("Failed to load provider: %s", e, exc_info=True)
            raise

    def list_providers(
        self, enabled_only: bool = False, available_only: bool = False
    ) -> List[DataProvider]:
        """
        Get list of registered providers

        Args:
            enabled_only: Only return enabled providers
            available_only: Only return available (enabled + healthy) providers

        Returns:
            List of providers sorted by priority
        """
        providers = list(self._providers.values())

        if enabled_only:
            providers = [p for p in providers if p.enabled]

        if available_only:
            providers = [p for p in providers if p.is_available()]

        # Sort by priority (lower = higher priority)
        providers.sort(key=lambda p: p.priority)

        return providers

    def get_provider(self, name: str) -> Optional[DataProvider]:
        """
        Get provider by name

        Args:
            name: Provider name

        Returns:
            DataProvider instance or None if not found
        """
        return self._providers.get(name)

    def next_available(
        self,
        query_type: Optional[QueryType] = None,
        exclude: Optional[List[str]] = None,
    ) -> Optional[DataProvider]:
        """
        Get next available provider based on priority

        Args:
            query_type: Filter providers that support this query type
            exclude: Provider names to exclude

        Returns:
            Next available provider or None
        """
        exclude = exclude or []
        providers = self.list_providers(available_only=True)

        for provider in providers:
            if provider.name in exclude:
                continue

            # Check query type support if specified
            if query_type and not provider.capabilities().supports(query_type):
                continue

            # Refresh health if needed
            self._refresh_health_if_needed(provider)

            # Check if still healthy after refresh
            if provider.is_available():
                return provider

        return None

    def get_providers_by_query_type(self, query_type: QueryType) -> List[DataProvider]:
        """
        Get all providers that support a specific query type

        Args:
            query_type: Query type to filter by

        Returns:
            List of providers sorted by priority
        """
        providers = self.list_providers(available_only=True)
        return [p for p in providers if p.capabilities().supports(query_type)]

    def health_check_all(self, force: bool = False) -> Dict[str, HealthStatus]:
        """
        Check health of all providers

        Args:
            force: Force check even if cache is fresh

        Returns:
            Dict mapping provider names to health status
        """
        results = {}

        for name, provider in self._providers.items():
            if force or self._should_refresh_health(name):
                try:
                    health = provider.health()
                    self._health_cache[name] = health
                    self._last_health_check[name] = datetime.now()
                    results[name] = health

                    # Update statistics
                    self._update_health_stats()

                except Exception as e:
                    logger.error("Health check failed for %s: %s", name, e)
                    error_health = HealthStatus(
                        status=ProviderStatus.UNHEALTHY,
                        last_check=datetime.now(),
                        error_message=str(e),
                    )
                    self._health_cache[name] = error_health
                    results[name] = error_health
            else:
                # Use cached health
                results[name] = self._health_cache.get(
                    name,
                    HealthStatus(
                        status=ProviderStatus.UNKNOWN, last_check=datetime.now()
                    ),
                )

        return results

    def _refresh_health_if_needed(self, provider: DataProvider):
        """Refresh provider health if cache expired"""
        if self._should_refresh_health(provider.name):
            try:
                health = provider.health()
                self._health_cache[provider.name] = health
                self._last_health_check[provider.name] = datetime.now()
            except Exception as e:
                logger.warning("Health check failed for %s: %s", provider.name, e)

    def _should_refresh_health(self, provider_name: str) -> bool:
        """Check if health cache for provider is expired"""
        last_check = self._last_health_check.get(provider_name)

        if not last_check:
            return True

        elapsed = (datetime.now() - last_check).total_seconds()
        return elapsed > self.health_check_interval

    def _update_health_stats(self):
        """Update health statistics"""
        self._stats["healthy_providers"] = sum(
            1 for h in self._health_cache.values() if h.status == ProviderStatus.HEALTHY
        )
        self._stats["degraded_providers"] = sum(
            1
            for h in self._health_cache.values()
            if h.status == ProviderStatus.DEGRADED
        )
        self._stats["unhealthy_providers"] = sum(
            1
            for h in self._health_cache.values()
            if h.status == ProviderStatus.UNHEALTHY
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        return {
            **self._stats,
            "config_path": self.config_path,
            "health_check_interval": self.health_check_interval,
            "provider_count": len(self._providers),
        }

    def reload_configuration(self):
        """Reload configuration and reinitialize providers"""
        logger.info("Reloading configuration...")

        # Clear existing providers
        self._providers.clear()
        self._health_cache.clear()
        self._last_health_check.clear()

        # Reset stats
        self._stats = {
            "total_providers": 0,
            "enabled_providers": 0,
            "healthy_providers": 0,
            "degraded_providers": 0,
            "unhealthy_providers": 0,
            "total_queries": 0,
            "failed_queries": 0,
        }

        # Reload
        self._load_configuration()

        logger.info("Configuration reloaded successfully")

    def __str__(self):
        return (
            f"DataSourceRegistry(providers={len(self._providers)}, "
            f"enabled={self._stats['enabled_providers']})"
        )

    def __repr__(self):
        return self.__str__()
