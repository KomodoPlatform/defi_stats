#!/usr/bin/env python3
import requests
from fastapi.testclient import TestClient
from main import app
from util.logger import logger

client = TestClient(app)


swagger = requests.get("http://0.0.0.0:7068/openapi.json").json()
endpoints = swagger["paths"].keys()
logger.info(endpoints)


def test_swagger_endpoints():
    num_endpoints = len(endpoints)
    x = 1
    for i in endpoints:
        i = i.replace("{ticker_id}", "KMD_LTC")
        i = i.replace("{pair_str}", "KMD_LTC")
        i = i.replace("{market_pair}", "KMD_LTC")
        i = i.replace("{days_in_past}", "3")
        i = i.replace("{uuid}", "77777777-2762-4633-8add-6ad2e9b1a4e7")
        i = i.replace("{coin}", "KMD")
        i = i.replace("{ticker}", "KMD")
        i = i.replace("{category}", "gui")
        if i.endswith("ticker_for_ticker"):
            i = f"{i}/?ticker=KMD"
        if i.endswith("distinct"):
            i = f"{i}/?coin=KMD"
        logger.calc(f"Testing {i}  [{x}/{num_endpoints}]...")
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
        x += 1
