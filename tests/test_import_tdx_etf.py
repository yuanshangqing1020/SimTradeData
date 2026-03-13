"""Tests for ETF support in TDX binary importer."""

import struct

import pytest

from scripts.import_tdx_day import (
    RECORD_FORMAT,
    is_stock_code,
    parse_tdx_day_file,
)


def _make_record(date_int, open_p, high, low, close, amount, volume):
    """Build a single TDX binary record for testing."""
    return struct.pack(RECORD_FORMAT, date_int, open_p, high, low, close, amount, volume, 0)


@pytest.mark.unit
class TestIsStockCodeEtf:
    """is_stock_code should accept ETF filenames."""

    def test_sh_etf(self):
        assert is_stock_code("sh510050.day") is True

    def test_sz_etf(self):
        assert is_stock_code("sz159919.day") is True

    def test_sh_lof(self):
        assert is_stock_code("sh500018.day") is True

    def test_sz_lof(self):
        assert is_stock_code("sz161039.day") is True

    def test_sh_stock(self):
        assert is_stock_code("sh600000.day") is True

    def test_sz_stock(self):
        assert is_stock_code("sz000001.day") is True

    def test_sh_index_rejected(self):
        assert is_stock_code("sh000001.day") is False

    def test_sz_index_rejected(self):
        assert is_stock_code("sz399001.day") is False


@pytest.mark.unit
class TestParseTdxDayFileEtf:
    """parse_tdx_day_file should apply correct divisor for ETFs."""

    def test_stock_default_divisor(self):
        data = _make_record(20250101, 1050, 1100, 1000, 1080, 1e8, 500000)
        df = parse_tdx_day_file(data)
        assert len(df) == 1
        assert df.iloc[0]["close"] == pytest.approx(10.80)
        assert df.iloc[0]["open"] == pytest.approx(10.50)

    def test_etf_divisor_1000(self):
        data = _make_record(20250101, 1050, 1100, 1000, 1080, 1e8, 500000)
        df = parse_tdx_day_file(data, price_divisor=1000.0)
        assert len(df) == 1
        assert df.iloc[0]["close"] == pytest.approx(1.080)
        assert df.iloc[0]["open"] == pytest.approx(1.050)
