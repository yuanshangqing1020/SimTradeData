import pytest

from simtradedata.router.exceptions import NoSourceAvailable
from simtradedata.router.smart_router import SmartRouter


class TestDetectMarket:
    def test_shanghai_ss(self):
        router = SmartRouter()
        assert router._detect_market("600000.SS") == "cn"

    def test_shenzhen_sz(self):
        router = SmartRouter()
        assert router._detect_market("000001.SZ") == "cn"

    def test_us(self):
        router = SmartRouter()
        assert router._detect_market("AAPL.US") == "us"

    def test_unknown_suffix_raises(self):
        router = SmartRouter()
        with pytest.raises(ValueError, match="Unknown market suffix"):
            router._detect_market("1234.XX")


class TestResolveSources:
    def test_daily_bars_cn(self):
        router = SmartRouter()
        sources = router._resolve_sources("daily_bars", "cn")
        assert sources == ["mootdx", "eastmoney", "baostock"]

    def test_daily_bars_us(self):
        router = SmartRouter()
        sources = router._resolve_sources("daily_bars", "us")
        assert sources == ["yfinance"]

    def test_no_source_raises(self):
        router = SmartRouter()
        with pytest.raises(NoSourceAvailable):
            router._resolve_sources("minute_bars", "us")

    def test_unknown_data_type_raises(self):
        router = SmartRouter()
        with pytest.raises(NoSourceAvailable):
            router._resolve_sources("nonexistent", "cn")

    def test_custom_config(self):
        custom = {"daily_bars": {"cn": ["eastmoney"]}}
        router = SmartRouter(config=custom)
        assert router._resolve_sources("daily_bars", "cn") == ["eastmoney"]
