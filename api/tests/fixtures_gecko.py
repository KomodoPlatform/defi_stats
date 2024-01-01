#!/usr/bin/env python3
import os
import sys
API_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(API_ROOT_PATH)


def setup_gecko_ticker_item():
    yield {
        "ticker_id": "KMD_LTC",
        "pool_id": "KMD_LTC",
        "base_currency": "KMD",
        "target_currency": "LTC",
        "last_price": "2.0000000000",
        "base_volume": "100.0000000000",
        "target_volume": "120.0000000000",
        "bid": "1.2300000000",
        "ask": "4.5600000000",
        "high": "7.890000000000",
        "low": "0.123400000000",
        "liquidity_in_usd": "1000000.0000000000",
    }
