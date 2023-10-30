#!/usr/bin/env python3
import json
import requests
from logger import logger
from utils import Utils
from generics import Files, Templates
from const import API_ROOT_PATH


class DexAPI:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.utils = Utils(testing=self.testing)
        self.files = Files(self.testing)
        self.templates = Templates()
        self.coins_config = self.utils.load_jsonfile(
            self.files.coins_config_file)
        self.mm2_host = "http://127.0.0.1:7783"

    def api(self, params: dict) -> dict:
        try:
            r = requests.post(self.mm2_host, json=params)
            return json.loads(r.text)["result"]
        except Exception as e:  # pragma: no cover
            if "CoinConfigNotFound" in r.text:
                resp = json.loads(r.text)
                err = {
                    "error": f"CoinConfigNotFound",
                    "message": r.text
                }
                logger.error(f"CoinConfigNotFound for {params['params']}")
            err = {
                "error": f"{e}",
                "message": r.text
            }
            # logger.error(err)
            return err

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    def orderbook(self, pair: tuple) -> dict:
        try:
            base = pair[0]
            quote = pair[1]
            # Check if pair is in coins config
            if len(set(pair).intersection(set(self.coins_config.keys()))) != 2:
                if base not in self.coins_config.keys():
                    err = {
                        "error": f"CoinConfigNotFound for {base}"
                    }
                else:
                    err = {
                        "error": f"CoinConfigNotFound for {quote}"
                    }
                logger.warning(err)
                return err
            if self.testing:
                orderbook = f"{API_ROOT_PATH}/tests/fixtures/orderbook/{base}_{quote}.json"
                return self.utils.load_jsonfile(orderbook)

            params = {
                "mmrpc": "2.0",
                "method": "orderbook",
                "params": {
                    "base": pair[0],
                    "rel": pair[1]
                },
                "id": 42
            }
            return self.api(params)

        except Exception as e:  # pragma: no cover
            err = {
                "error": f"Error in [DexAPI.orderbook] for {pair}: {e}"
            }
            logger.error(err)
            return err
