#!/usr/bin/env python3
import time
from const import API_ROOT_PATH


class Files:
    def __init__(self, testing: bool = False):
        if testing:
            folder = f"{API_ROOT_PATH}/tests/fixtures"
        else:
            folder = f"{API_ROOT_PATH}/cache"
        # Coins repo data
        self.coins_file = f"{folder}/coins"
        self.coins_config_file = f"{folder}/coins_config.json"
        # For CoinGecko endpoints
        self.gecko_source_file = f"{folder}/gecko/source_cache.json"
        self.gecko_tickers_file = f"{folder}/gecko/ticker_cache.json"
        self.gecko_pairs_file = f"{folder}/gecko/pairs_cache.json"


class Time:
    def __init__(self, testing: bool = False):
        self.testing = testing

    def now(self):  # pragma: no cover
        return int(time.time())

    def hours_ago(self, num):
        return int(time.time()) - (num * 60 * 60)

    def days_ago(self, num):
        return int(time.time()) - (num * 60 * 60) * 24


class Templates:
    def __init__(self):
        pass

    def gecko_info(self, coin_id):
        return {
            "usd_market_cap": 0,
            "usd_price": 0,
            "coingecko_id": coin_id
        }

    def volumes_and_prices(self, suffix):
        return {
            "base_volume": 0,
            "quote_volume": 0,
            f"highest_price_{suffix}": 0,
            f"lowest_price_{suffix}": 0,
            "last_price": 0,
            "last_trade": 0,
            "trades_24hr": 0,
            f"price_change_percent_{suffix}": 0,
            f"price_change_{suffix}": 0,
        }

    def orderbook(self, base: str, quote: str, v2=False):
        data = {
            "pair": f"{base}_{quote}",
            "base": base,
            "quote": quote,
            "bids": [],
            "asks": [],
            "total_asks_base_vol": 0,
            "total_asks_quote_vol": 0,
            "total_bids_base_vol": 0,
            "total_bids_quote_vol": 0
        }
        if v2:  # pragma: no cover
            data.update({
                "total_asks_base_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_asks_quote_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_bids_base_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_bids_quote_vol": {
                    "decimal": 0
                }
            })
        return data
