#!/usr/bin/env python3
from fastapi.testclient import TestClient
import pytest
from main import app
from logger import logger

client = TestClient(app)


def test_gecko_pairs_endpoint():
    r = client.get("/api/v3/gecko/pairs")
    assert r.status_code == 200
    data = r.json()
    ticker_list = [i["ticker_id"] for i in data]
    assert "KMD_LTC" in ticker_list
    assert "LTC_KMD" not in ticker_list
    assert isinstance(data, list)
    for i in data:
        assert isinstance(i, dict)
        assert i["ticker_id"] == i["pool_id"]
        split = i["ticker_id"].split("_")
        assert split[0] == i["base"]
        assert split[1] == i["target"]
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
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_orderbook_endpoint():
    a = client.get("/api/v3/gecko/orderbook/KMD_LTC?netid=0000")
    assert a.status_code == 422
    b = client.get("/api/v3/gecko/orderbook/KMD_LTC?netid=7777")
    assert b.status_code == 200
    c = client.get("/api/v3/gecko/orderbook/KMD_LTC?netid=8762")
    assert c.status_code == 200
    r = client.get("/api/v3/gecko/orderbook/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert len(b.json()["bids"]) + len(b.json()["asks"]) != len(c.json()["bids"]) + len(
        c.json()["asks"]
    )
    assert b.json()["bids"][0][0] == r.json()["bids"][0][0]
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
    """
    Uses actual live data, so values
    are not known before tests are run
    """
    x = client.get("/api/v3/gecko/historical_trades/KMD_LTC?netid=0000")
    assert x.status_code == 422
    a = client.get("/api/v3/gecko/historical_trades/KMD_LTC?netid=7777")
    assert a.status_code == 200
    b = client.get("/api/v3/gecko/historical_trades/KMD_LTC?netid=8762")
    assert a.status_code == 200
    assert a.json()["average_price"] != b.json()["average_price"]
    r = client.get("/api/v3/gecko/historical_trades/KMD_LTC")
    assert a.json()["average_price"] == r.json()["average_price"]
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert isinstance(data["buy"], list)
    for i in data["buy"]:
        logger.info(i)
    for i in data["sell"]:
        logger.info(i)
    if len(data["buy"]) > 0:
        assert isinstance(data["buy"][0]["price"], str)
        assert isinstance(data["buy"][0]["trade_id"], str)
        assert isinstance(data["buy"][0]["timestamp"], str)
        assert isinstance(data["buy"][0]["base_volume"], str)
        assert isinstance(data["buy"][0]["target_volume"], str)
    assert isinstance(data["sell"], list)
    if len(data["sell"]) > 0:
        assert isinstance(data["sell"][0]["price"], str)
        assert isinstance(data["sell"][0]["trade_id"], str)
        assert isinstance(data["sell"][0]["timestamp"], str)
        assert isinstance(data["sell"][0]["base_volume"], str)
        assert isinstance(data["sell"][0]["target_volume"], str)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_get_swap():
    """
    Returns a single swap from the database
    """
    r = client.get("/api/v3/swaps/swap/39236a1b-7777-7777-7777-5fe039064e8d")
    assert r.status_code == 406
    r = client.get("/api/v3/swaps/swap/c15c7839-0951-445b-84d5-6128167d0b0a")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert data["uuid"] == "c15c7839-0951-445b-84d5-6128167d0b0a"
    assert data["maker_amount"] == "123"
    assert data["taker_amount"] == "6.4715968209"
    assert data["taker_coin_ticker"] == "KMD"
    assert data["started_at"] == "1701309262"

    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data
