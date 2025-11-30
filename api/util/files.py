import os
import time
import json
import requests
from const import API_ROOT_PATH
from util.logger import timed, logger
import util.defaults as default
import util.validate as validate


class Files:
    def __init__(self):
        if os.getenv("IS_TESTING") == "True" == "True":
            folder = f"{API_ROOT_PATH}/tests/fixtures"
            self.foo = f"{folder}/foo.json"
            self.bar = f"{folder}/bar.json"
        else:  # pragma: no cover
            folder = f"{API_ROOT_PATH}/cache"

        # External source cache
        self.coins = f"{folder}/coins/coins.json"
        self.coins_config = f"{folder}/coins/coins_config.json"
        self.gecko_source = f"{folder}/gecko/source.json"
        self.fixer_rates = f"{folder}/rates/fixer_rates.json"
        self.cmc_assets_source = f"{folder}/cmc/assets_source.json"
        self.cmc_summary = f"{folder}/cmc/summary.json"
        self.cmc_assets = f"{folder}/cmc/assets.json"

        # Top Fives
        self.adex_alltime = f"{folder}/generic/adex_alltime.json"
        self.adex_weekly = f"{folder}/generic/adex_weekly.json"
        self.adex_fortnite = f"{folder}/generic/adex_fortnite.json"
        self.adex_24hr = f"{folder}/generic/adex_24hr.json"

        # Foundational cache
        self.coin_volumes_24hr = f"{folder}/coins/volumes_24hr.json"
        self.coin_volumes_alltime = f"{folder}/coins/volumes_alltime.json"
        self.coin_swaps_alltime = f"{folder}/coins/swaps_alltime.json"
        self.pairs_last_traded = f"{folder}/pairs/last_traded.json"
        self.pairs_orderbook_extended = f"{folder}/pairs/orderbook_extended.json"
        self.pair_prices_24hr = f"{folder}/pairs/prices_24hr.json"
        self.pair_volumes_24hr = f"{folder}/pairs/volumes_24hr.json"
        self.pair_volumes_14d = f"{folder}/pairs/volumes_14d.json"
        self.pair_volumes_alltime = f"{folder}/pairs/volumes_alltime.json"

        # REVIEW
        # self.generic_summary = f"{folder}/generic/summary.json"
        # self.generic_tickers = f"{folder}/generic/tickers.json"
        # self.generic_tickers_14d = f"{folder}/generic/tickers_14d.json"

        # For Prices endpoints
        self.prices_tickers_v1 = f"{folder}/prices/tickers_v1.json"
        self.prices_tickers_v2 = f"{folder}/prices/tickers_v2.json"

        self.markets_summary = f"{folder}/markets/summary.json"
        self.stats_api_summary = f"{folder}/stats_api/summary.json"
        self.tickers = f"{folder}/generic/tickers.json"
        self.gecko_pairs = f"{folder}/gecko/pairs.json"

    def get_cache_fn(self, name):
        return getattr(self, name, None)

    def save_json(self, fn, data, indent=4):
        try:
            if len(data) > 0:
                if validate.json_obj(data):
                    with open(fn, "w+") as f:
                        if indent == 0:
                            json.dump(data, f, separators=(',', ':'))
                        else:
                            json.dump(data, f, indent=indent)
                        return {
                            "result": "success",
                            "msg": f"Saved {fn}",
                            "loglevel": "saved",
                            "ignore_until": 0,
                        }
                else:
                    return {
                        "result": "error",
                        "msg": f"Not saving {fn}, data not valid json! Data: ",
                        "loglevel": "warning",
                        "ignore_until": 0,
                    }
            else:
                return {
                    "result": "error",
                    "msg": f"Not saving {fn}, data is empty",
                    "loglevel": "warning",
                    "ignore_until": 0,
                }

        except Exception as e:
            logger.warning(e)
            logger.warning(data)
            return {
                "result": "error",
                "msg": f"Not saving {fn}, error with the data: {e}",
                "loglevel": "warning",
                "ignore_until": 0,
            }

    @timed
    def load_jsonfile(self, path):
        i = 0
        while i < 5:
            try:
                with open(path, "r") as f:
                    return default.result(
                        data=json.load(f),
                        msg=f"Loaded {path}",
                        loglevel="saved",
                        ignore_until=3,
                    )
            except Exception as e:  # pragma: no cover
                error = f"Error loading {path}: {e}"
            i += 1
            time.sleep(0.2)
        logger.warning(error)
        return None

    def download_json(self, url):
        try:
            data = requests.get(url).json()
            return data
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")


files = Files()
