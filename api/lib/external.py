#!/usr/bin/env python3
import requests
import time
from const import FIXER_API_KEY
from util.files import Files
from util.helper import get_chunks
from util.logger import logger
import util.defaults as default
import util.templates as template
import util.memcache as memcache


class CoinGeckoAPI:
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.files = Files()
            # logger.loop("Getting gecko_source for CoinGeckoAPI")
        except Exception as e:  # pragma: no cover
            logger.error({"error": f"{type(e)} Failed to init CoinGeckoAPI: {e}"})

    def get_gecko_coin_ids(self) -> list:
        coins_config = memcache.get_coins_config()
        coin_ids = list(
            set(
                [
                    coins_config[i]["coingecko_id"]
                    for i in coins_config
                    if coins_config[i]["coingecko_id"]
                    not in ["na", "test-coin", ""]
                ]
            )
        )
        coin_ids.sort()
        return coin_ids

    def get_gecko_info(self):
        coins_info = {}
        coins_config = memcache.get_coins_config()
        for coin in coins_config:
            native_coin = coin.split("-")[0]
            coin_id = coins_config[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""]:
                coins_info.update({coin: template.gecko_info(coin_id)})
                if native_coin not in coins_info:
                    coins_info.update({native_coin: template.gecko_info(coin_id)})
        return coins_info

    def get_gecko_coins(self, gecko_info: dict, coin_ids: list):
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})
        for coin in gecko_info:
            coin_id = gecko_info[coin]["coingecko_id"]
            gecko_coins[coin_id].append(coin)
        return gecko_coins

    def get_gecko_source(self):  # pragma: no cover
        if self.testing:
            return self.files.load_jsonfile(self.gecko_source)
        param_limit = 200
        coin_ids = self.get_gecko_coin_ids()
        gecko_info = self.get_gecko_info()
        gecko_coins = self.get_gecko_coins(gecko_info, coin_ids)
        coin_id_chunks = list(get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                # logger.debug(f"Coingecko chunk url: {url}")
                r = requests.get(url)
                if r.status_code != 200:
                    raise Exception(f"Invalid response: {r.status_code}")
                gecko_source = r.json()
            except Exception as e:
                msg = f"Failed for url: {url}!"
                return default.error(e, msg)
            for coin_id in gecko_source:
                try:
                    coins = gecko_coins[coin_id]
                    for coin in coins:
                        if "usd" in gecko_source[coin_id]:
                            gecko_info[coin].update(
                                {"usd_price": gecko_source[coin_id]["usd"]}
                            )
                        if "usd_market_cap" in gecko_source[coin_id]:
                            gecko_info[coin].update(
                                {
                                    "usd_market_cap": gecko_source[coin_id][
                                        "usd_market_cap"
                                    ]
                                }
                            )
                except Exception as e:
                    error = f"{type(e)}: CoinGecko ID request/response mismatch [{coin_id}] [{e}]"
                    logger.warning(error)
            time.sleep(0.1)
        return gecko_info


class FixerAPI:  # pragma: no cover
    def __init__(self):
        self.base_url = "http://data.fixer.io/api"
        self.api_key = FIXER_API_KEY

    def latest(self):
        try:
            return requests.get("https://rates.komodo.earth/api/v1/usd_rates").json()
            '''
            if self.api_key == "":
                raise ApiKeyNotFoundException("FIXER_API key not set!")
            url = f"{self.base_url}/latest?access_key={self.api_key}"
            r = requests.get(url)
            received_rates = r.json()
            received_rates["date"] = str(
                datetime.fromtimestamp(received_rates["timestamp"])
            )
            if "error" in received_rates:
                raise Exception(received_rates["error"])
            usd_eur_rate = received_rates["rates"]["USD"]
            for rate in received_rates["rates"]:
                received_rates["rates"][rate] /= usd_eur_rate
            received_rates["base"] = "USD"
            received_rates["rates"]["USD"] = 1.0
            received_rates["rates"].pop("BTC")
            return received_rates
            '''
        except Exception as e:
            return default.error(e)


class BinanceAPI:  # pragma: no cover
    def __init__(self):
        self.base_url = "https://data-api.binance.vision"

    def ticker_price(self):
        endpoint = "api/v3/ticker/price"
        r = requests.get(f"{self.base_url}/{endpoint}")
        return r.json()
