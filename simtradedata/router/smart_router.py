"""SmartRouter: unified data access layer with automatic source selection."""

import importlib
import logging

import pandas as pd

from simtradedata.router.exceptions import DataSourceError, NoSourceAvailable
from simtradedata.router.route_config import DEFAULT_ROUTE_TABLE, FETCHER_REGISTRY

logger = logging.getLogger(__name__)

_MARKET_SUFFIXES = {
    "SS": "cn",
    "SZ": "cn",
    "BJ": "cn",
    "US": "us",
}


class SmartRouter:
    """Unified data access layer with automatic source selection and fallback."""

    def __init__(self, config: dict = None):
        self._config = config or DEFAULT_ROUTE_TABLE
        self._fetchers: dict = {}

    # -- Lifecycle --

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for name, fetcher in self._fetchers.items():
            try:
                fetcher.logout()
            except Exception as e:
                logger.warning("Failed to logout %s: %s", name, e)
        self._fetchers.clear()
        return False

    # -- Internal helpers --

    @staticmethod
    def _detect_market(symbol: str) -> str:
        """Detect market from PTrade symbol suffix.

        Args:
            symbol: PTrade format code (e.g. '600000.SS', 'AAPL.US').

        Returns:
            Market identifier: 'cn' or 'us'.

        Raises:
            ValueError: If the suffix is not recognized.
        """
        suffix = symbol.rsplit(".", 1)[-1]
        market = _MARKET_SUFFIXES.get(suffix)
        if market is None:
            raise ValueError(f"Unknown market suffix: .{suffix}")
        return market

    def _resolve_sources(self, data_type: str, market: str) -> list[str]:
        """Look up the prioritized source list for a data_type and market.

        Raises:
            NoSourceAvailable: If no sources are configured.
        """
        routes = self._config.get(data_type, {})
        sources = routes.get(market)
        if not sources:
            raise NoSourceAvailable(
                f"No source configured for {data_type}/{market}"
            )
        return sources

    def _get_fetcher(self, source_name: str):
        """Return a cached fetcher instance, creating it on first access."""
        if source_name not in self._fetchers:
            cls_path = FETCHER_REGISTRY.get(source_name)
            if cls_path is None:
                raise NoSourceAvailable(
                    f"No fetcher registered for '{source_name}'"
                )
            module_path, cls_name = cls_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            cls = getattr(module, cls_name)
            fetcher = cls()
            fetcher.login()
            self._fetchers[source_name] = fetcher
        return self._fetchers[source_name]

    @staticmethod
    def _is_source_healthy(fetcher) -> bool:
        """Check circuit breaker health, tolerating fetchers without one."""
        cb = getattr(fetcher, "_circuit_breaker", None)
        if cb is None:
            # MootdxUnifiedFetcher wraps an inner fetcher; check it
            inner = getattr(fetcher, "_quotes_fetcher", None)
            if inner is not None:
                cb = getattr(inner, "_circuit_breaker", None)
        if cb is None:
            return True
        return cb.is_available()

    # -- Core fallback mechanism --

    def _try_fetch(self, data_type, fetch_from, symbol=None, market=None):
        """Try each source in priority order, falling back on failure.

        Args:
            data_type: Route table key (e.g. 'daily_bars').
            fetch_from: Callable(fetcher, source_name) -> pd.DataFrame.
            symbol: PTrade symbol for auto market detection.
            market: Explicit market override (used when no symbol).

        Returns:
            DataFrame from the first successful source.

        Raises:
            DataSourceError: If all sources fail with exceptions.
            NoSourceAvailable: If no sources are configured.
        """
        if market is None:
            if symbol is not None:
                market = self._detect_market(symbol)
            else:
                market = "cn"

        sources = self._resolve_sources(data_type, market)
        errors = []

        for source_name in sources:
            fetcher = self._get_fetcher(source_name)

            if not self._is_source_healthy(fetcher):
                logger.debug("Skipping %s: circuit breaker open", source_name)
                continue

            try:
                result = fetch_from(fetcher, source_name)
                if result is not None and not result.empty:
                    logger.info(
                        "Fetched %s from %s (%d rows)",
                        data_type, source_name, len(result),
                    )
                    return result
                logger.debug(
                    "Empty result from %s for %s", source_name, data_type,
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch %s from %s: %s",
                    data_type, source_name, e,
                )
                errors.append((source_name, e))

        if errors:
            detail = "; ".join(f"{s}: {e}" for s, e in errors)
            raise DataSourceError(
                f"All sources failed for {data_type}: {detail}"
            )
        return pd.DataFrame()

    # -- Public API --

    def get_daily_bars(self, symbol, start_date, end_date):
        """Fetch daily OHLCV bars with automatic source selection."""

        def fetch_from(fetcher, source_name):
            if source_name == "mootdx":
                return fetcher.fetch_daily_data(symbol, start_date, end_date)
            elif source_name == "eastmoney":
                return fetcher.fetch_daily_bars(symbol, start_date, end_date)
            elif source_name == "baostock":
                df = fetcher.fetch_unified_daily_data(
                    symbol, start_date, end_date,
                )
                if df.empty:
                    return df
                cols = [
                    "date", "open", "high", "low", "close", "volume", "amount",
                ]
                return df[[c for c in cols if c in df.columns]]
            elif source_name == "yfinance":
                result = fetcher.fetch_batch_ohlcv(
                    [symbol], start_date, end_date,
                )
                return result.get(symbol, pd.DataFrame())
            raise ValueError(f"Unknown source for daily_bars: {source_name}")

        return self._try_fetch("daily_bars", fetch_from, symbol=symbol)
