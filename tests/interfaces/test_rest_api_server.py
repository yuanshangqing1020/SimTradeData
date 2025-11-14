import datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from simtradedata.config import Config
from simtradedata.interfaces.rest_api import RESTAPIServer


@pytest.fixture()
def api_router():
    router = MagicMock()
    payload = {
        "data": [{"symbol": "000001.SZ", "name": "平安银行", "market": "SZ"}],
        "count": 1,
        "timestamp": datetime.datetime.now().isoformat(),
    }
    router.get_stock_info.return_value = payload
    router.get_history.return_value = {
        "data": [],
        "count": 0,
        "timestamp": datetime.datetime.now().isoformat(),
    }
    router.get_fundamentals.return_value = {
        "data": [],
        "count": 0,
        "timestamp": datetime.datetime.now().isoformat(),
    }
    router.get_snapshot.return_value = payload
    router.get_api_stats.return_value = {
        "cache": {},
        "formatter": {},
        "config": {},
        "builders": {},
    }
    return router


@pytest.fixture()
def rest_client(api_router):
    db_manager = MagicMock()
    config = Config()
    server = RESTAPIServer(db_manager, api_router, config)
    return TestClient(server.app)


def test_health_endpoint(rest_client):
    response = rest_client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert "timestamp" in payload


def test_list_stocks_calls_router(rest_client, api_router):
    response = rest_client.get(
        "/api/v1/stocks", params={"market": "SZ", "fields": "symbol,name"}
    )
    assert response.status_code == 200
    api_router.get_stock_info.assert_called_once()
    kwargs = api_router.get_stock_info.call_args.kwargs
    assert kwargs["market"] == "SZ"
    assert kwargs["fields"] == ["symbol", "name"]
    assert kwargs["format_type"] == "dict"


def test_history_endpoint_passthrough(rest_client, api_router):
    response = rest_client.get("/api/v1/stocks/000001.SZ/history")
    assert response.status_code == 200
    api_router.get_history.assert_called_once()
    kwargs = api_router.get_history.call_args.kwargs
    assert kwargs["symbols"] == "000001.SZ"
    assert kwargs["format_type"] == "dict"


def test_error_payload_translates_to_400(rest_client, api_router):
    api_router.get_stock_info.return_value = {
        "error": True,
        "error_message": "invalid request",
        "timestamp": datetime.datetime.now().isoformat(),
    }
    response = rest_client.get("/api/v1/stocks")
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid request"


def test_legacy_price_endpoint_alias(rest_client, api_router):
    response = rest_client.get(
        "/api/v1/stocks/000001.SZ/price",
        params={
            "start_date": "2024-01-01",
            "end_date": "2024-01-10",
            "frequency": "1d",
        },
    )
    assert response.status_code == 200
    api_router.get_history.assert_called()
