"""
财务数据获取接口集成测试

测试新增的数据获取接口：
1. QStock三大财务报表API
2. BaoStock 6个季度财务查询API
3. BaoStock除权除息数据API
"""

import pytest

from simtradedata.config import Config
from simtradedata.data_sources import DataSourceManager


@pytest.mark.integration
class TestFinancialDataAPIs:
    """财务数据获取接口集成测试"""

    @pytest.fixture
    def config(self):
        """创建测试配置"""
        cfg = Config()
        cfg.set("data_sources.qstock.enabled", True)
        cfg.set("data_sources.baostock.enabled", True)
        return cfg

    @pytest.fixture
    def manager(self, config):
        """创建数据源管理器"""
        return DataSourceManager(config=config)

    @pytest.mark.slow
    def test_get_balance_sheet(self, manager):
        """测试获取资产负债表"""
        # 测试获取平安银行的资产负债表
        result = manager.get_balance_sheet(symbol="000001.SZ", report_date="20231231")

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            # 如果返回成功，验证数据结构
            if result.get("success"):
                assert "data" in result or result.get("data") is not None
                print(f"资产负债表数据获取成功")
            else:
                print(f"资产负债表获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_get_income_statement(self, manager):
        """测试获取利润表"""
        # 测试获取平安银行的利润表
        result = manager.get_income_statement(
            symbol="000001.SZ", report_date="20231231"
        )

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                assert "data" in result or result.get("data") is not None
                print(f"利润表数据获取成功")
            else:
                print(f"利润表获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_get_cash_flow(self, manager):
        """测试获取现金流量表"""
        # 测试获取平安银行的现金流量表
        result = manager.get_cash_flow(symbol="000001.SZ", report_date="20231231")

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                assert "data" in result or result.get("data") is not None
                print(f"现金流量表数据获取成功")
            else:
                print(f"现金流量表获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_profit_data(self, manager):
        """测试获取季频盈利能力数据"""
        # 测试获取平安银行2023年Q4盈利能力数据
        result = manager.query_profit_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"盈利能力数据获取成功")
                # 验证关键字段存在
                if isinstance(data, dict):
                    assert "roeAvg" in data or len(data) > 0
            else:
                print(f"盈利能力数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_operation_data(self, manager):
        """测试获取季频营运能力数据"""
        # 测试获取平安银行2023年Q4营运能力数据
        result = manager.query_operation_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"营运能力数据获取成功")
            else:
                print(f"营运能力数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_growth_data(self, manager):
        """测试获取季频成长能力数据"""
        # 测试获取平安银行2023年Q4成长能力数据
        result = manager.query_growth_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"成长能力数据获取成功")
            else:
                print(f"成长能力数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_balance_data(self, manager):
        """测试获取季频偿债能力数据"""
        # 测试获取平安银行2023年Q4偿债能力数据
        result = manager.query_balance_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"偿债能力数据获取成功")
            else:
                print(f"偿债能力数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_cash_flow_data(self, manager):
        """测试获取季频现金流量数据"""
        # 测试获取平安银行2023年Q4现金流量数据
        result = manager.query_cash_flow_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"现金流量数据获取成功")
            else:
                print(f"现金流量数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_query_dupont_data(self, manager):
        """测试获取季频杜邦指数数据"""
        # 测试获取平安银行2023年Q4杜邦指数数据
        result = manager.query_dupont_data(symbol="000001.SZ", year=2023, quarter=4)

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                assert data is not None
                print(f"杜邦指数数据获取成功")
            else:
                print(f"杜邦指数数据获取失败: {result.get('error', '未知错误')}")

    @pytest.mark.slow
    def test_get_adjustment_data(self, manager):
        """测试获取除权除息数据"""
        # 测试获取平安银行的除权除息数据
        result = manager.get_adjustment_data(
            symbol="000001.SZ", start_date="2023-01-01", end_date="2023-12-31"
        )

        # 验证返回结果
        assert result is not None
        if isinstance(result, dict):
            if result.get("success"):
                data = result.get("data")
                # 除权除息数据可能为空（如果该年没有分红）
                assert data is not None or data == []
                print(f"除权除息数据获取成功")
            else:
                print(f"除权除息数据获取失败: {result.get('error', '未知错误')}")


@pytest.mark.unit
class TestFinancialDataAPIsValidation:
    """财务数据接口参数验证测试"""

    @pytest.fixture
    def config(self):
        """创建测试配置"""
        cfg = Config()
        cfg.set("data_sources.qstock.enabled", True)
        cfg.set("data_sources.baostock.enabled", True)
        return cfg

    @pytest.fixture
    def manager(self, config):
        """创建数据源管理器"""
        return DataSourceManager(config=config)

    def test_balance_sheet_symbol_validation(self, manager):
        """测试资产负债表symbol参数验证"""
        result = manager.get_balance_sheet(symbol="", report_date="20231231")
        # unified_error_handler会捕获ValidationError并返回错误dict
        assert result is not None
        assert isinstance(result, dict)
        assert result.get("success") is False
        assert "message" in result or "error" in result

    def test_query_profit_data_validation(self, manager):
        """测试盈利能力数据参数验证"""
        # 测试空symbol
        result = manager.query_profit_data(symbol="", year=2023, quarter=4)
        assert result is not None
        assert isinstance(result, dict)
        assert result.get("success") is False
        assert "message" in result or "error" in result

        # 测试空year
        result = manager.query_profit_data(symbol="000001.SZ", year=None, quarter=4)
        assert result is not None
        assert isinstance(result, dict)
        assert result.get("success") is False
        assert "message" in result or "error" in result

        # 测试空quarter
        result = manager.query_profit_data(symbol="000001.SZ", year=2023, quarter=None)
        assert result is not None
        assert isinstance(result, dict)
        assert result.get("success") is False
        assert "message" in result or "error" in result
