#!/usr/bin/env python3
import time
from collections import OrderedDict
from decimal import Decimal
from logger import logger
from const import MM2_HOST, CoinConfigNotFoundCoins
from generics import Files, Templates, CoinNotFoundException
from utils import Utils
from dex_api import DexAPI
from helper import valid_coins
from validate import validate_coin


class Orderbook:
    def __init__(self, pair, testing: bool = False, mm2_host=MM2_HOST, mm2_port=7877):
        self.pair = pair
        self.base = pair.base
        self.quote = pair.quote
        self.mm2_host = mm2_host
        self.mm2_port = mm2_port
        self.mm2_rpc = f"{mm2_host}:{mm2_port}"
        self.testing = testing
        self.files = Files(testing=self.testing)
        self.utils = Utils(testing=self.testing)
        self.templates = Templates()
        self.dexapi = DexAPI(testing=self.testing, mm2_host=mm2_host, mm2_port=mm2_port)
        self.gecko_source = self.utils.load_jsonfile(self.files.gecko_source_file)
        self.base_is_segwit_coin = self.base in self.utils.segwit_coins()
        self.quote_is_segwit_coin = self.quote in self.utils.segwit_coins()
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config_file)

    def for_pair(self, endpoint=False, depth=10000000, reverse=False):
        try:
            orderbook_data = OrderedDict()
            orderbook_data["ticker_id"] = self.pair.as_str
            orderbook_data["base"] = self.pair.base
            orderbook_data["quote"] = self.pair.quote
            if reverse:
                orderbook_data[
                    "ticker_id"
                ] = f'{self.pair.as_str.split("_")[1]}_{self.pair.as_str.split("_")[0]}'
                orderbook_data["base"] = self.pair.quote
                orderbook_data["quote"] = self.pair.base
            orderbook_data["timestamp"] = f"{int(time.time())}"
            data = self.get_and_parse(endpoint, reverse)
            if data is not None:
                orderbook_data["bids"] = data["bids"][:depth][::-1]
                orderbook_data["asks"] = data["asks"][::-1][:depth]
                if endpoint:
                    total_bids_base_vol = sum(
                        [Decimal(i[1]) for i in orderbook_data["bids"]]
                    )
                    total_asks_base_vol = sum(
                        [Decimal(i[1]) for i in orderbook_data["asks"]]
                    )
                    total_bids_quote_vol = sum(
                        [Decimal(i[0]) * Decimal(i[1]) for i in orderbook_data["bids"]]
                    )
                    total_asks_quote_vol = sum(
                        [Decimal(i[0]) * Decimal(i[1]) for i in orderbook_data["asks"]]
                    )
                else:
                    total_bids_base_vol = sum(
                        [Decimal(i["base_max_volume"]) for i in orderbook_data["bids"]]
                    )
                    total_asks_base_vol = sum(
                        [Decimal(i["base_max_volume"]) for i in orderbook_data["asks"]]
                    )
                    total_bids_quote_vol = sum(
                        [
                            Decimal(i["base_max_volume"]) * Decimal(i["price"])
                            for i in orderbook_data["bids"]
                        ]
                    )
                    total_asks_quote_vol = sum(
                        [
                            Decimal(i["base_max_volume"]) * Decimal(i["price"])
                            for i in orderbook_data["asks"]
                        ]
                    )
                orderbook_data["total_asks_base_vol"] = total_asks_base_vol
                orderbook_data["total_bids_base_vol"] = total_bids_base_vol
                orderbook_data["total_asks_quote_vol"] = total_asks_quote_vol
                orderbook_data["total_bids_quote_vol"] = total_bids_quote_vol
                orderbook_data["total_asks_base_usd"] = (
                    total_asks_base_vol * self.pair.base_price
                )
                orderbook_data["total_bids_quote_usd"] = (
                    total_bids_quote_vol * self.pair.quote_price
                )
                if reverse:
                    orderbook_data["total_asks_base_usd"] = (
                        total_asks_base_vol * self.pair.quote_price
                    )
                    orderbook_data["total_bids_quote_usd"] = (
                        total_bids_quote_vol * self.pair.base_price
                    )

                orderbook_data["liquidity_usd"] = (
                    orderbook_data["total_asks_base_usd"]
                    + orderbook_data["total_bids_quote_usd"]
                )
            else:
                orderbook_data["total_asks_base_vol"] = 0
                orderbook_data["total_bids_base_vol"] = 0
                orderbook_data["total_asks_quote_vol"] = 0
                orderbook_data["total_bids_quote_vol"] = 0
                orderbook_data["liquidity_usd"] = 0

            return orderbook_data
        except Exception as e:  # pragma: no cover
            logger.warning(f"Error getting orderbook for {self.pair.as_str}: {e}")
            if "bids" not in orderbook_data:
                orderbook_data["bids"] = []
            if "asks" not in orderbook_data:
                orderbook_data["asks"] = []
            orderbook_data["total_asks_base_vol"] = 0
            orderbook_data["total_bids_base_vol"] = 0
            orderbook_data["total_asks_quote_vol"] = 0
            orderbook_data["total_bids_quote_vol"] = 0
            orderbook_data["total_asks_base_usd"] = 0
            orderbook_data["total_asks_quote_usd"] = 0
            orderbook_data["total_bids_base_usd"] = 0
            orderbook_data["liquidity_usd"] = 0
            orderbook_data["total_bids_quote_usd"] = 0
            return orderbook_data

    def get_and_parse(self, endpoint=False, reverse=False):
        try:
            base = self.pair.base
            quote = self.pair.quote
            pair_set = {base, quote}

            # Handle segwit only coins
            if self.base_is_segwit_coin and base not in self.coins_config.keys():
                base = f"{self.pair.base}-segwit"
            if self.quote_is_segwit_coin and quote not in self.coins_config.keys():
                quote = f"{self.pair.quote}-segwit"
            if reverse:
                orderbook = self.templates.orderbook(self.pair.quote, self.pair.base)
                pair = (quote, base)
            else:
                orderbook = self.templates.orderbook(self.pair.base, self.pair.quote)
                pair = (base, quote)
        except CoinNotFoundException as e:
            pass
        except Exception as e:
            logger.muted(f"{type(e)} Error: {e}")

        try:            
            base = self.pair.base
            validate_coin(base, self.coins_config)
            quote = self.pair.quote
            validate_coin(quote, self.coins_config)
            
            x = self.dexapi.orderbook(pair)
            for i in ["asks", "bids"]:
                if "error" not in x:
                    orderbook[i] += x[i]
                else:
                    if pair_set.intersection(set(CoinConfigNotFoundCoins)) == 0:
                        logger.debug(f"No orderbook for {base}/{quote}")


        except CoinNotFoundException as e:
            pass
        except Exception as e:
            logger.muted(f"{type(e)} Error: {e}")

        try:
            bids_converted_list = []
            asks_converted_list = []
            for bid in orderbook["bids"]:
                if endpoint:
                    bids_converted_list.append(
                        [bid["price"]["decimal"], bid["base_max_volume"]["decimal"]]
                    )
                else:
                    bids_converted_list.append(
                        {
                            "price": bid["price"]["decimal"],
                            "base_max_volume": bid["base_max_volume"]["decimal"],
                        }
                    )

            for ask in orderbook["asks"]:
                if endpoint:
                    asks_converted_list.append(
                        [ask["price"]["decimal"], ask["base_max_volume"]["decimal"]]
                    )
                else:
                    asks_converted_list.append(
                        {
                            "price": ask["price"]["decimal"],
                            "base_max_volume": ask["base_max_volume"]["decimal"],
                        }
                    )
            orderbook["bids"] = bids_converted_list
            orderbook["asks"] = asks_converted_list
        except CoinNotFoundException as e:
            pass
        except Exception as e:
            logger.muted(f"{type(e)} Error: {e}")
        return orderbook
