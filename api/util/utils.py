#!/usr/bin/env python3
import time
import json
import inspect
import requests
from random import randrange
from typing import Any
from decimal import Decimal, InvalidOperation
from util.logger import logger
from util.helper import format_10f, get_stopwatch, get_trace
from util.files import Files


class Utils:
    def __init__(self, testing: bool = False):
        self.testing = testing
        self.files = Files(testing=self.testing)

    def load_jsonfile(self, path, attempts=5, fallback=None):
        i = 0
        start = int(time.time())
        while True:
            i += 1
            try:
                with open(path, "r") as f:
                    end = int(time.time())
                    return json.load(f)
            except Exception as e:  # pragma: no cover
                err = {"error": f"Error loading {path}: {e}"}
                if i >= attempts:
                    end = int(time.time())
                    if fallback:
                        return fallback
                    get_stopwatch(start, context=f"utils.load_jsonfile | Loading {path}")
                    return err
                time.sleep(0.1)

    def download_json(self, url):
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            data = requests.get(url).json()
            # get_stopwatch(start, muted=True, context=f"{url} response recieved")
            return data
        except Exception as e:  # pragma: no cover
            error = f"{type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return None

    def round_to_str(self, value: Any, rounding=8):
        try:
            if isinstance(value, (str, int, float)):
                value = Decimal(value)
            if isinstance(value, Decimal):
                value = value.quantize(Decimal(f'1.{"0" * rounding}'))
            else:
                raise TypeError(f"Invalid type: {type(value)}")
        except (ValueError, TypeError, InvalidOperation) as e:  # pragma: no cover
            logger.debug(f"{type(e)} Error rounding {value}: {e}")
            value = 0
        return f"{value:.{rounding}f}"

    def clean_decimal_dict_list(self, data, to_string=False, rounding=8):
        """
        Works for a list of dicts with no nesting
        (e.g. summary_cache.json)
        """
        for i in data:
            for j in i:
                if isinstance(i[j], Decimal):
                    if to_string:
                        i[j] = self.round_to_str(i[j], rounding)
                    else:
                        i[j] = round(float(i[j]), rounding)
        return data

    def clean_decimal_dict(self, data, to_string=False, rounding=8):
        """
        Works for a simple dict with no nesting
        (e.g. summary_cache.json)
        """
        for i in data:
            if isinstance(data[i], Decimal):
                if to_string:
                    data[i] = self.round_to_str(data[i], rounding)
                else:
                    data[i] = float(data[i])
        return data

    def get_suffix(self, days: int) -> str:
        if days == 1:
            return "24h"
        else:
            return f"{days}d"

    def segwit_coins(self) -> list:
        coins = self.load_jsonfile(self.files.coins)
        segwit_coins = [
            i["coin"].split("-")[0] for i in coins if i["coin"].endswith("-segwit")
        ]
        return segwit_coins

    def get_related_coins(self, coin, exclude_segwit=True):
        try:
            coin = coin.split("-")[0]
            coins = self.load_jsonfile(self.files.coins)
            data = [
                i["coin"]
                for i in coins
                if i["coin"] == coin or i["coin"].startswith(f"{coin}-")
            ]
            if exclude_segwit:
                data = [i for i in data if "-segwit" not in i]
            return data
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error getting related coins for {coin}: {e}")
            return []

    def get_related_pairs(self, pair: tuple):
        coin_a = pair.as_tuple[0]
        coin_b = pair.as_tuple[1]
        coins_a = self.get_related_coins(coin_a, exclude_segwit=True)
        coins_b = self.get_related_coins(coin_b, exclude_segwit=True)
        return [(i, j) for i in coins_a for j in coins_b if i != j]

    def get_chunks(self, data, chunk_length):
        for i in range(0, len(data), chunk_length):
            yield data[i : i + chunk_length]

    def get_gecko_usd_price(self, coin: str, gecko_source) -> float:
        try:
            return Decimal(gecko_source[coin.split("-")[0]]["usd_price"])
        except KeyError:  # pragma: no cover
            return Decimal(0)

    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        lowest = 0
        try:
            for ask in orderbook["asks"]:
                price = ask["price"]
                # This might already be decimal if not direct from mm2
                if not isinstance(ask["price"], Decimal):
                    if "decimal" in ask["price"]:
                        price = Decimal(ask["price"]["decimal"])
                    else:
                        price = Decimal(ask["price"])
                if lowest == 0:
                    lowest = price
                elif Decimal(price) < Decimal(lowest):
                    lowest = price
        except KeyError as e:  # pragma: no cover
            logger.error(e)
        return format_10f(Decimal(lowest))

    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        highest = 0
        try:
            for bid in orderbook["bids"]:
                price = bid["price"]
                # This might already be decimal if not direct from mm2
                if not isinstance(bid["price"], Decimal):
                    if "decimal" in bid["price"]:
                        price = Decimal(bid["price"]["decimal"])
                    else:
                        price = Decimal(bid["price"])
                if highest == 0:
                    highest = price
                elif Decimal(price) > Decimal(highest):
                    highest = price
        except KeyError as e:  # pragma: no cover
            logger.error(e)
        return format_10f(Decimal(highest))
