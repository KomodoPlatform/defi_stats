#!/usr/bin/env python3
import time
import requests
from fastapi.testclient import TestClient
import pytest
from main import app
from util.logger import logger
from util.transform import derive

client = TestClient(app)


swagger = requests.get("http://0.0.0.0:7068/openapi.json").json()
endpoints = swagger["paths"].keys()
logger.info(endpoints)

'''
def test_swagger_endpoints():
    for i in endpoints:
        i = i.replace("{ticker_id}", "KMD_LTC")
        i = i.replace("{pair_str}", "KMD_LTC")
        i = i.replace("{market_pair}", "KMD_LTC")
        i = i.replace("{days_in_past}", "3")
        i = i.replace("{uuid}", "77777777-2762-4633-8add-6ad2e9b1a4e7")
        i = i.replace("{coin}", "KMD")
        i = i.replace("{ticker}", "KMD")
        i = i.replace("{category}", "gui")
        if i.endswith('ticker_for_ticker'):
            i = f'{i}/?ticker=KMD'
        logger.loop(i)
        if i.endswith("distinct"):
            i = f'{i}/?coin=KMD'
            logger.loop(i)
        logger.calc(f"Testing {i}...")
        r = client.get(i)
        if '"error"' in r.text:
            logger.warning(f"{r.text}")
            logger.warning(f"{i} failed...")
        elif r.status_code != 200:
            logger.warning(f"{r.status_code}")
            logger.warning(f"{i} failed...")
        else:
            logger.query(f"{i} ok!")
        assert r.status_code == 200


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
        base, quote = derive.base_quote(i["ticker_id"])
        assert base == i["base"]
        assert quote == i["target"]
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
    r = client.get("/api/v3/gecko/orderbook/KMD_LTC")
    assert r.status_code == 200
    time.sleep(1)
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
    """
    Uses actual live data, so values
    are not known before tests are run
    """
    r = client.get("/api/v3/gecko/historical_trades/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert isinstance(data["buy"], list)
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
'''


def test_get_swap():
    """
    Returns a single swap from the database
    """
    r = client.get("/api/v3/swaps/swap/39236a1b-7777-7777-7777-5fe039064e8d")
    assert r.status_code == 400
    r = client.get("/api/v3/swaps/swap/FFFFFFFF-ee4b-494f-a2fb-48467614b613")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert data["uuid"] == "FFFFFFFF-ee4b-494f-a2fb-48467614b613"
    assert data["maker_amount"] == 10
    assert data["taker_amount"] == 1
    assert data["taker_coin_ticker"] == "DOGE"

    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data
