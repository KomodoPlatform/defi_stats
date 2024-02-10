import time
import json
import requests
from const import API_ROOT_PATH, IS_TESTING
from util.logger import timed, logger
import util.defaults as default
import util.memcache as memcache
import util.validate as validate


class Files:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.options = []
        default.params(self, self.kwargs, self.options)
        if IS_TESTING:
            folder = f"{API_ROOT_PATH}/tests/fixtures"
            self.foo = f"{folder}/foo.json"
            self.bar = f"{folder}/bar.json"
        else:  # pragma: no cover
            folder = f"{API_ROOT_PATH}/cache"

        # Coins repo data
        self.coins = f"{folder}/coins/coins.json"
        self.coins_config = f"{folder}/coins/coins_config.json"

        # For Rates endpoints
        self.fixer_rates = f"{folder}/rates/fixer_rates.json"

        # For CoinGecko endpoints
        self.gecko_source = f"{folder}/gecko/source.json"

        # FOUNDATIONAL CACHE
        self.adex_fortnite = f"{folder}/generic/adex_fortnite.json"
        self.coin_volumes_24hr = f"{folder}/generic/coin_volumes_24hr.json"
        self.pair_last_traded = f"{folder}/generic/pair_last_traded.json"
        self.pair_orderbook_extended = f"{folder}/generic/pair_orderbook_extended.json"
        self.pair_volumes_24hr = f"{folder}/generic/pair_volumes_24hr.json"

        # MARKETS CACHE

        # REVIEW
        self.generic_summary = f"{folder}/generic/summary.json"
        self.generic_tickers = f"{folder}/generic/tickers.json"
        self.generic_tickers_14d = f"{folder}/generic/tickers_14d.json"

        # For Prices endpoints
        self.prices_tickers_v1 = f"{folder}/prices/tickers_v1.json"
        self.prices_tickers_v2 = f"{folder}/prices/tickers_v2.json"

    def get_cache_fn(self, name):
        return getattr(self, name, None)

    @timed
    def save_json(self, fn, data):
        try:
            if len(data) > 0:
                if validate.json_obj(data):
                    with open(fn, "w+") as f:
                        json.dump(data, f, indent=4)
                        return {
                            "result": "success",
                            "msg": f"{fn} saved!",
                            "loglevel": "saved",
                            "ignore_until": 5
                        }
                else:
                    return {
                        "result": "error",
                        "msg": f"Not saving {fn}, data not valid json! Data: ",
                        "loglevel": "warning",
                    }
            else:
                return {
                    "result": "error",
                    "msg": f"Not saving {fn}, data is empty",
                    "loglevel": "warning",
                }

        except Exception as e:
            logger.warning(e)
            logger.warning(data)
            return {
                "result": "error",
                "msg": f"Not saving {fn}, error with the data: {e}",
                "loglevel": "warning",
            }

    def load_jsonfile(self, path):
        i = 0
        while i < 5:
            try:
                with open(path, "r") as f:
                    # logger.calc(f"Loading {path}")
                    return json.load(f)
            except Exception as e:  # pragma: no cover
                error = f"Error loading {path}: {e}"
                if memcache.get("testing") is None:
                    logger.warning(error)
            i += 1
            time.sleep(0.1)
        return None

    def download_json(self, url):
        try:
            data = requests.get(url).json()
            return data
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")
