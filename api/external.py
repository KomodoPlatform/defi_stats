#!/usr/bin/env python3
import requests
from datetime import datetime
from logger import logger
from generics import Files, Templates
from utils import Utils
from generics import ApiKeyNotFoundException
from const import FIXER_API_KEY


class CoinGeckoAPI:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.utils = Utils(testing=self.testing)
        self.templates = Templates()
        self.files = Files(self.testing)
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config_file)
        self.gecko_source = self.load_gecko_source()
        self.priced_coins = sorted(list(self.gecko_source.keys()))

    def load_gecko_source(self):
        try:
            return self.utils.load_jsonfile(self.files.gecko_source_file)
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [CoinGeckoAPI.load_gecko_source]: {e}")
            return {}

    def get_gecko_coin_ids_list(self) -> list:
        coin_ids = list(
            set(
                [
                    self.coins_config[i]["coingecko_id"]
                    for i in self.coins_config
                    if self.coins_config[i]["coingecko_id"]
                    not in ["na", "test-coin", ""]
                ]
            )
        )
        coin_ids.sort()
        return coin_ids

    def get_gecko_info_dict(self):
        coins_info = {}
        for coin in self.coins_config:
            native_coin = coin.split("-")[0]
            coin_id = self.coins_config[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""]:
                coins_info.update({coin: self.templates.gecko_info(coin_id)})
                if native_coin not in coins_info:
                    coins_info.update({native_coin: self.templates.gecko_info(coin_id)})
        return coins_info

    def get_gecko_coins_dict(self, gecko_info: dict, coin_ids: list):
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})
        for coin in gecko_info:
            coin_id = gecko_info[coin]["coingecko_id"]
            gecko_coins[coin_id].append(coin)
        return gecko_coins

    def get_gecko_source(self):
        param_limit = 200
        coin_ids = self.get_gecko_coin_ids_list()
        coins_info = self.get_gecko_info_dict()
        gecko_coins = self.get_gecko_coins_dict(coins_info, coin_ids)
        coin_id_chunks = list(self.utils.get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                r = requests.get(url)
                if r.status_code != 200:  # pragma: no cover
                    raise Exception(f"Invalid response: {r.status_code}")
                gecko_source = r.json()

            except Exception as e:  # pragma: no cover
                error = {"error": f"{type(e)} Error in [get_gecko_source]: {e}"}
                logger.error(error)
                return error
            try:
                for coin_id in gecko_source:
                    try:
                        coins = gecko_coins[coin_id]
                        for coin in coins:
                            if "usd" in gecko_source[coin_id]:
                                coins_info[coin].update(
                                    {"usd_price": gecko_source[coin_id]["usd"]}
                                )
                            if "usd_market_cap" in gecko_source[coin_id]:
                                coins_info[coin].update(
                                    {
                                        "usd_market_cap": gecko_source[coin_id][
                                            "usd_market_cap"
                                        ]
                                    }
                                )
                    except Exception as e:  # pragma: no cover
                        error = (
                            f"CoinGecko ID request/response mismatch [{coin_id}] [{e}]"
                        )
                        logger.warning(error)

            except Exception as e:  # pragma: no cover
                error = {"error": f"{type(e)} Error in [get_gecko_source]: {e}"}
                logger.error(error)
                return error
        return coins_info


class FixerAPI:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.files = Files(self.testing)

    def get_fixer_rates_source(self):
        try:
            if FIXER_API_KEY == "":
                raise ApiKeyNotFoundException("FIXER_API key not set!")
            r = requests.get(
                f"http://data.fixer.io/api/latest?access_key={FIXER_API_KEY}"
            )
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

        except Exception as e:
            return {"error": f"{e}"}
