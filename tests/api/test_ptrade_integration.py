"""
Test PTradeAdapter integration with FastAPI RESTAPIServer

Verifies that PTradeAdapter can work alongside FastAPI endpoints
and that the call chain is properly established.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from simtradedata.api import APIRouter
from simtradedata.config import Config
from simtradedata.database import DatabaseManager
from simtradedata.interfaces import PTradeAPIAdapter, RESTAPIServer


class TestPTradeAdapterIntegration:
    """Test PTradeAdapter integration with RESTAPIServer"""

    @pytest.fixture
    def mock_db_manager(self):
        """Create mock database manager"""
        db = Mock(spec=DatabaseManager)

        # Mock stock info data
        db.fetchall.return_value = [
            {
                "symbol": "000001.SZ",
                "name": "平安银行",
                "market": "SZ",
                "industry_l1": "金融",
                "industry_l2": "银行",
                "list_date": "1991-04-03",
                "status": "active",
                "is_st": 0,
            }
        ]

        return db

    @pytest.fixture
    def api_components(self, mock_db_manager):
        """Create API components"""
        config = Config()
        api_router = APIRouter(mock_db_manager, config)
        ptrade_adapter = PTradeAPIAdapter(mock_db_manager, api_router, config)
        rest_server = RESTAPIServer(mock_db_manager, api_router, config)

        return {
            "db": mock_db_manager,
            "router": api_router,
            "ptrade": ptrade_adapter,
            "rest": rest_server,
        }

    def test_ptrade_adapter_uses_api_router(self, api_components):
        """Test that PTradeAdapter uses APIRouter for data access"""
        ptrade = api_components["ptrade"]

        # Verify PTradeAdapter has access to APIRouter
        assert ptrade.api_router is not None
        assert isinstance(ptrade.api_router, APIRouter)

        # Verify PTradeAdapter has access to DatabaseManager
        assert ptrade.db_manager is not None
        assert isinstance(ptrade.db_manager, Mock)

    def test_rest_server_uses_api_router(self, api_components):
        """Test that RESTAPIServer uses APIRouter"""
        rest = api_components["rest"]

        # Verify RESTAPIServer has access to APIRouter
        assert rest.api_router is not None
        assert isinstance(rest.api_router, APIRouter)

    def test_ptrade_adapter_get_stock_list(self, api_components):
        """Test PTradeAdapter get_stock_list method"""
        ptrade = api_components["ptrade"]

        # Call PTradeAdapter method
        result = ptrade.get_stock_list(market="SZ")

        # Verify result is DataFrame
        assert isinstance(result, pd.DataFrame)

    def test_rest_api_stock_list_endpoint(self, api_components):
        """Test RESTAPIServer stock list endpoint"""
        rest_server = api_components["rest"]
        client = TestClient(rest_server.app)

        # Call REST API endpoint
        response = client.get("/api/v1/stocks?market=SZ&limit=100")

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert "data" in data or "success" in data or isinstance(data, list)

    def test_ptrade_and_rest_use_same_data_source(self, api_components):
        """Test that PTradeAdapter and RESTAPIServer use the same data source"""
        ptrade = api_components["ptrade"]
        rest = api_components["rest"]

        # Both should use the same APIRouter instance
        assert ptrade.api_router is rest.api_router

        # Both should use the same DatabaseManager
        assert ptrade.db_manager is rest.db_manager

    def test_ptrade_adapter_get_price(self, api_components):
        """Test PTradeAdapter get_price method"""
        ptrade = api_components["ptrade"]
        db = api_components["db"]

        # Mock price data
        db.fetchall.return_value = [
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-20",
                "frequency": "1d",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000000,
                "money": 10200000,
            }
        ]

        # Call method
        result = ptrade.get_price(
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Verify result
        assert isinstance(result, pd.DataFrame)

    def test_rest_api_history_endpoint(self, api_components):
        """Test RESTAPIServer history endpoint"""
        rest_server = api_components["rest"]
        db = api_components["db"]
        client = TestClient(rest_server.app)

        # Mock price data
        db.fetchall.return_value = [
            {
                "symbol": "000001.SZ",
                "trade_date": "2024-01-20",
                "frequency": "1d",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000000,
                "money": 10200000,
            }
        ]

        # Call endpoint
        response = client.get(
            "/api/v1/stocks/000001.SZ/history"
            "?start_date=2024-01-01&end_date=2024-01-31"
        )

        # Verify response
        assert response.status_code == 200

    def test_call_chain_integrity(self, api_components):
        """Test the complete call chain: RestAPI -> APIRouter -> DBManager"""
        rest = api_components["rest"]
        router = api_components["router"]
        db = api_components["db"]

        # Verify call chain
        assert rest.api_router is router
        assert router.db_manager is db

        # Same for PTradeAdapter
        ptrade = api_components["ptrade"]
        assert ptrade.api_router is router
        assert ptrade.db_manager is db

    def test_middleware_integration_with_endpoints(self, api_components):
        """Test that middleware works with API endpoints"""
        rest_server = api_components["rest"]
        client = TestClient(rest_server.app)

        # Test health endpoint (should work without auth)
        response = client.get("/api/v1/health")
        assert response.status_code == 200

        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"


class TestPTradeAdapterAsDepedency:
    """Test using PTradeAdapter as FastAPI dependency"""

    def test_ptrade_adapter_can_be_used_as_dependency(self):
        """Test that PTradeAdapter can be injected as FastAPI dependency"""
        from fastapi import Depends, FastAPI
        from fastapi.testclient import TestClient

        # Create mock components
        db = Mock(spec=DatabaseManager)
        db.fetchall.return_value = [
            {"symbol": "000001.SZ", "name": "平安银行", "market": "SZ"}
        ]

        config = Config()
        router = APIRouter(db, config)
        ptrade = PTradeAPIAdapter(db, router, config)

        # Create FastAPI app with PTradeAdapter as dependency
        app = FastAPI()

        def get_ptrade_adapter() -> PTradeAPIAdapter:
            return ptrade

        @app.get("/ptrade/stocks")
        def get_stocks_ptrade(adapter: PTradeAPIAdapter = Depends(get_ptrade_adapter)):
            """PTrade-style endpoint using dependency injection"""
            result = adapter.get_stock_list()
            return {"success": True, "count": len(result)}

        # Test the endpoint
        client = TestClient(app)
        response = client.get("/ptrade/stocks")

        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert data["success"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
