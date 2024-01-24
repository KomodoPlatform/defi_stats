import time
import json
import requests
from const import API_ROOT_PATH
from util.defaults import default_error, set_params
from util.logger import timed, logger
import util.validate as validate


class Files:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.options = []
        set_params(self, self.kwargs, self.options)
        if self.testing:
            folder = f"{API_ROOT_PATH}/tests/fixtures"
            self.foo = f"{folder}/foo.json"
            self.bar = f"{folder}/bar.json"
        else:
            folder = f"{API_ROOT_PATH}/cache"
        # For Rates endpoints
        self.fixer_rates = f"{folder}/rates/fixer_io.json"
        # Coins repo data
        self.coins = f"{folder}/coins/coins.json"
        self.coins_config = f"{folder}/coins/coins_config.json"
        # For Stats API endpoints
        self.statsapi_adex_fortnite = f"{folder}/stats_api/adex_fortnite.json"
        self.statsapi_summary = f"{folder}/stats_api/summary.json"
        # For CoinGecko endpoints
        self.gecko_source = f"{folder}/gecko/source.json"
        self.gecko_tickers = f"{folder}/gecko/tickers.json"
        self.gecko_tickers_old = f"{folder}/gecko/tickers_old.json"
        # For Markets endpoints
        self.markets_tickers = f"{folder}/markets/tickers.json"
        # For Prices endpoints
        self.prices_tickers_v1 = f"{folder}/prices/tickers_v1.json"
        self.prices_tickers_v2 = f"{folder}/prices/tickers_v2.json"
        # For Generic Cache
        self.generic_last_traded = f"{folder}/generic/last_traded.json"
        self.generic_last_traded_old = f"{folder}/generic/last_traded_old.json"

        self.generic_pairs = f"{folder}/generic/pairs.json"
        self.generic_pairs_old = f"{folder}/generic/pairs_old.json"

        self.generic_tickers = f"{folder}/generic/tickers.json"
        self.generic_tickers_old = f"{folder}/generic/tickers_old.json"

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
                            "message": f"{fn} saved!",
                            "loglevel": "save",
                        }
                else:
                    return {
                        "result": "error",
                        "message": f"Not saving {fn}, data not valid json! Data: ",
                        "loglevel": "warning",
                    }
            else:
                return {
                    "result": "error",
                    "message": f"Not saving {fn}, data is empty",
                    "loglevel": "warning",
                }

        except Exception as e:
            return {
                "result": "error",
                "message": f"Not saving {fn}, error with the data: {e}",
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
                logger.warning(error)
            i += 1
            time.sleep(0.1)
        return None

    def download_json(self, url):
        try:
            data = requests.get(url).json()
            return data
        except Exception as e:  # pragma: no cover
            return default_error(e)
