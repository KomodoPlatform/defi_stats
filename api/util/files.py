import json
import requests
from const import API_ROOT_PATH
from util.defaults import default_error, set_params
from util.logger import timed
from util.validate import validate_json


class Files:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.options = ["testing", "netid"]
        set_params(self, self.kwargs, self.options)
        if self.testing:
            folder = f"{API_ROOT_PATH}/tests/fixtures"
            self.foo = f"{folder}/foo.json"
            self.bar = f"{folder}/bar.json"
        else:
            folder = f"{API_ROOT_PATH}/cache"
        # Coins repo data
        self.coins = f"{folder}/coins/coins.json"
        self.coins_config = f"{folder}/coins/coins_config.json"
        # For Markets endpoints
        self.markets_tickers = f"{folder}/markets/tickers_{self.netid}.json"
        self.markets_pairs = f"{folder}/markets/pairs_{self.netid}.json"
        self.markets_last_trade = f"{folder}/markets/last_trade_{self.netid}.json"
        # For CoinGecko endpoints
        self.gecko_source = f"{folder}/gecko/source.json"
        self.gecko_tickers = f"{folder}/gecko/tickers_{self.netid}.json"
        self.gecko_pairs = f"{folder}/gecko/pairs_{self.netid}.json"
        # For Rates endpoints
        self.fixer_rates = f"{folder}/rates/fixer_io.json"
        # For Prices endpoints
        self.prices_tickers_v1 = f"{folder}/prices/tickers_v1.json"
        self.prices_tickers_v2 = f"{folder}/prices/tickers_v2.json"

    def get_cache_fn(self, name):
        return getattr(self, name, None)

    @timed
    def save_json(self, fn, data):
        try:
            if len(data) > 0:
                if validate_json(data):
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
                        "message": f"Not saving {fn}, data is not valid json format!",
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
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:  # pragma: no cover
            error = f"Error loading {path}: {e}"
            return default_error(e, error)

    def download_json(self, url):
        try:
            data = requests.get(url).json()
            return data
        except Exception as e:
            return default_error(e)
