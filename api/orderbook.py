#!/usr/bin/env python3
import time
from collections import OrderedDict
from decimal import Decimal
from logger import logger
from const import MM2_HOST
from generics import Files, Templates
from utils import Utils
from dex_api import DexAPI


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
        pass

    def for_pair(self, endpoint=False, depth=10000000):
        try:
            orderbook_data = OrderedDict()
            orderbook_data["ticker_id"] = self.pair.as_str
            orderbook_data["base"] = self.pair.base
            orderbook_data["quote"] = self.pair.quote
            orderbook_data["timestamp"] = f"{int(time.time())}"
            data = self.get_and_parse(endpoint)
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
                    # logger.debug(f"Total bids: {orderbook_data['bids']}")
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
                orderbook_data["liquidity_usd"] = (
                    orderbook_data["total_asks_base_usd"]
                    + orderbook_data["total_bids_quote_usd"]
                )
            else:
                orderbook_data["total_asks_base_vol"] = 0
                orderbook_data["total_bids_base_vol"] = 0
                orderbook_data["total_asks_quote_vol"] = 0
                orderbook_data["total_bids_quote_vol"] = 0

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
            orderbook_data["total_bids_quote_usd"] = 0
            return orderbook_data

    def get_and_parse(self, endpoint=False):
        try:
            orderbook = self.templates.orderbook(self.pair.base, self.pair.quote)
            for i in ["asks", "bids"]:
                x = self.dexapi.orderbook(self.pair.as_tuple)
                if 'error' not in x:
                    orderbook[i] = x[i]
                else:
                    logger.debug(f"No orderbook for {self.pair.base}/{self.pair.quote}")

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
        except Exception as e:
            logger.error(f"Error: {e}")
        return orderbook
