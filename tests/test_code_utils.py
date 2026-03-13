"""Tests for stock code utility functions including ETF classification."""

import pytest

from simtradedata.utils.code_utils import (
    convert_to_ptrade_code,
    get_mootdx_market,
    get_price_divisor,
    get_security_type,
    is_etf_code,
)


@pytest.mark.unit
class TestIsEtfCode:
    """Tests for is_etf_code() ETF/LOF identification."""

    # -- Positive: bare codes for each ETF prefix --

    def test_prefix_15_bare(self):
        assert is_etf_code("159919") is True

    def test_prefix_16_bare(self):
        assert is_etf_code("161039") is True

    def test_prefix_50_bare(self):
        assert is_etf_code("500018") is True

    def test_prefix_51_bare(self):
        assert is_etf_code("510050") is True

    def test_prefix_52_bare(self):
        assert is_etf_code("520000") is True

    def test_prefix_56_bare(self):
        assert is_etf_code("560010") is True

    def test_prefix_58_bare(self):
        assert is_etf_code("580000") is True

    def test_prefix_59_bare(self):
        assert is_etf_code("590002") is True

    # -- Positive: PTrade codes with market suffix --

    def test_sz_etf_with_suffix(self):
        assert is_etf_code("159919.SZ") is True

    def test_sz_lof_with_suffix(self):
        assert is_etf_code("161039.SZ") is True

    def test_sh_etf_with_suffix(self):
        assert is_etf_code("510050.SS") is True

    def test_sh_lof_with_suffix(self):
        assert is_etf_code("500018.SS") is True

    # -- Negative: stocks and indices --

    def test_stock_600xxx(self):
        assert is_etf_code("600000") is False

    def test_stock_000xxx(self):
        assert is_etf_code("000001") is False

    def test_stock_300xxx(self):
        assert is_etf_code("300750") is False

    def test_index_399xxx(self):
        assert is_etf_code("399001") is False

    def test_stock_with_suffix(self):
        assert is_etf_code("600000.SS") is False


@pytest.mark.unit
class TestGetSecurityType:
    """Tests for get_security_type() classification."""

    # -- Stocks --

    def test_sh_stock(self):
        assert get_security_type("600000.SS") == "stock"

    def test_sz_stock(self):
        assert get_security_type("000001.SZ") == "stock"

    def test_bj_stock(self):
        assert get_security_type("430047.BJ") == "stock"

    def test_gem_stock(self):
        assert get_security_type("300750.SZ") == "stock"

    def test_bare_stock(self):
        assert get_security_type("600000") == "stock"

    # -- ETFs --

    def test_sz_etf(self):
        assert get_security_type("159919.SZ") == "etf"

    def test_sh_etf(self):
        assert get_security_type("510050.SS") == "etf"

    def test_bare_etf(self):
        assert get_security_type("510050") == "etf"

    # -- Indices --

    def test_sz_index_399(self):
        assert get_security_type("399001.SZ") == "index"

    def test_sh_index_000(self):
        # 000001.SS = SSE Composite Index (Shanghai)
        assert get_security_type("000001.SS") == "index"

    def test_bare_index_399(self):
        assert get_security_type("399001") == "index"

    # -- Edge: 000xxx without .SS is stock (e.g. 000001.SZ = Ping An Bank) --

    def test_000xxx_sz_is_stock(self):
        assert get_security_type("000001.SZ") == "stock"

    def test_000xxx_bare_is_stock(self):
        # Without a market suffix, 000xxx cannot be determined as index
        assert get_security_type("000001") == "stock"


@pytest.mark.unit
class TestGetPriceDivisor:
    """Tests for get_price_divisor() TDX price correction."""

    def test_stock_divisor(self):
        assert get_price_divisor("600000.SS") == 1.0

    def test_etf_divisor(self):
        assert get_price_divisor("510050.SS") == 10.0

    def test_sz_etf_divisor(self):
        assert get_price_divisor("159919.SZ") == 10.0

    def test_lof_divisor(self):
        assert get_price_divisor("161039.SZ") == 10.0

    def test_bare_stock_divisor(self):
        assert get_price_divisor("000001") == 1.0

    def test_bare_etf_divisor(self):
        assert get_price_divisor("510050") == 10.0


@pytest.mark.unit
class TestConvertToPtradeCodeEtf:
    """Tests for convert_to_ptrade_code with ETF/LOF codes."""

    def test_sz_etf_159919(self):
        assert convert_to_ptrade_code("159919", "qstock") == "159919.SZ"

    def test_sz_lof_161039(self):
        assert convert_to_ptrade_code("161039", "qstock") == "161039.SZ"

    def test_sh_etf_510050(self):
        assert convert_to_ptrade_code("510050", "qstock") == "510050.SS"

    def test_sh_lof_500018(self):
        assert convert_to_ptrade_code("500018", "qstock") == "500018.SS"

    # -- Existing stock conversions still work --

    def test_sh_stock(self):
        assert convert_to_ptrade_code("600000", "qstock") == "600000.SS"

    def test_sz_stock(self):
        assert convert_to_ptrade_code("000001", "qstock") == "000001.SZ"

    def test_gem_stock(self):
        assert convert_to_ptrade_code("300750", "qstock") == "300750.SZ"


@pytest.mark.unit
class TestGetMootdxMarketEtf:
    """Tests for get_mootdx_market with ETF codes."""

    def test_sz_etf_returns_0(self):
        # 159919 starts with 1, SZ market = 0
        assert get_mootdx_market("159919.SZ") == 0

    def test_sh_etf_returns_1(self):
        # 510050 starts with 5, SH market = 1
        assert get_mootdx_market("510050.SS") == 1

    def test_sz_lof_returns_0(self):
        assert get_mootdx_market("161039.SZ") == 0

    def test_sh_lof_returns_1(self):
        assert get_mootdx_market("500018.SS") == 1
