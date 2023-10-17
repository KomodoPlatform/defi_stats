#!/usr/bin/env python3
from fastapi.testclient import TestClient
from decimal import Decimal
import pytest
from fixtures import setup_endpoints
from main import app

client = TestClient(app)


def test_gecko_pairs_endpoint():
    r = client.get("/api/v3/gecko/pairs")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    assert "ticker_id" in data[0]
    assert "pool_id" in data[0]
    assert "base" in data[0]
    assert "target" in data[0]
    split = data[0]["ticker_id"].split("_")
    assert split[0] == data[0]["base"]
    assert split[1] == data[0]["target"]
    assert data[0]["ticker_id"] == data[0]["pool_id"]
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_gecko_tickers_endpoint():
    r = client.get("/api/v3/gecko/tickers")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert "last_update" in data
    assert "pairs_count" in data
    assert "swaps_count" in data
    assert "combined_liquidity_usd" in data
    assert "combined_volume_usd" in data
    assert "ticker_id" in data["data"][0]
    assert "base_currency" in data["data"][0]
    assert "target_currency" in data["data"][0]
    assert "last_price" in data["data"][0]
    assert "base_volume" in data["data"][0]
    assert "bid" in data["data"][0]
    assert "ask" in data["data"][0]
    assert "high" in data["data"][0]
    assert "low" in data["data"][0]
    assert "liquidity_in_usd" in data["data"][0]
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_orderbook_endpoint():
    r = client.get("/api/v3/gecko/orderbook/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert data != {}
    assert "asks" in data
    assert "bids" in data
    assert len(data["asks"]) > 0
    assert len(data["bids"]) > 0
    assert len(data["asks"][0]) == 2
    assert len(data["bids"][0]) == 2
    assert isinstance(data["asks"][0], list)
    assert isinstance(data["bids"][0], list)
    assert isinstance(data["asks"][0][0], str)
    assert isinstance(data["bids"][0][0], str)
    assert isinstance(data["asks"][0][1], str)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_historical_trades_endpoint():
    r = client.get("/api/v3/gecko/historical_trades/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert isinstance(data["buy"], list)
    assert isinstance(data["buy"][0]["price"], str)
    assert isinstance(data["buy"][0]["trade_id"], str)
    assert isinstance(data["buy"][0]["timestamp"], str)
    assert isinstance(data["buy"][0]["base_volume"], str)
    assert isinstance(data["buy"][0]["target_volume"], str)
    assert isinstance(data["sell"], list)
    assert isinstance(data["sell"][0]["price"], str)
    assert isinstance(data["sell"][0]["trade_id"], str)
    assert isinstance(data["sell"][0]["timestamp"], str)
    assert isinstance(data["sell"][0]["base_volume"], str)
    assert isinstance(data["sell"][0]["target_volume"], str)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_gecko_pairs(setup_endpoints):
    endpoints = setup_endpoints
    result = endpoints.gecko_pairs()
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert "ticker_id" in result[0]
    assert "pool_id" in result[0]
    assert "base" in result[0]
    assert "target" in result[0]
    split = result[0]["ticker_id"].split("_")
    assert split[0] == result[0]["base"]
    assert split[1] == result[0]["target"]
    assert result[0]["ticker_id"] == result[0]["pool_id"]
    assert len(result) == len(set(i["ticker_id"] for i in result))
