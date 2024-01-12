#!/usr/bin/env python3
import time
from collections import OrderedDict
from decimal import Decimal
from lib.dex_api import DexAPI
from util.files import Files
from util.logger import logger, timed
from util.defaults import default_error, set_params, default_result
from util.transform import format_10f, reverse_ticker
from util.validate import validate_orderbook_pair
import util.templates as template
import lib


class Orderbook:
    def __init__(self, pair_obj, **kwargs):
        try:
            self.kwargs = kwargs
            self.pair = pair_obj
            self.base = self.pair.base
            self.quote = self.pair.quote
            self.options = ["testing", "netid", "mm2_host"]
            set_params(self, self.kwargs, self.options)
            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                self.gecko_source = lib.load_gecko_source()

            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                self.coins_config = lib.load_coins_config()

            if "last_traded_cache" in kwargs:
                self.last_traded_cache = kwargs["last_traded_cache"]
            else:
                logger.loop(
                    f"Getting generic_last_traded source for {self.pair} Orderbook"
                )
                self.last_traded_cache = lib.load_generic_last_traded()

            self.files = Files(testing=self.testing)
            segwit_coins = [i.coin for i in lib.COINS.with_segwit]
            self.base_is_segwit_coin = self.base in segwit_coins
            self.quote_is_segwit_coin = self.quote in segwit_coins
            self.dexapi = DexAPI(
                testing=self.testing, mm2_host=self.mm2_host, netid=self.netid
            )
            self.orderbook_template = template.orderbook(
                self.pair.as_str, self.pair.inverse_requested
            )
        except Exception as e:  # pragma: no cover
            logger.error({"error": f"{type(e)} Failed to init Orderbook: {e}"})

    @timed
    def for_pair(self, depth=100):
        try:
            orderbook_data = OrderedDict()
            if self.pair.inverse_requested:
                orderbook_data["ticker_id"] = reverse_ticker(self.pair.as_str)
                orderbook_data["base"] = self.pair.quote
                orderbook_data["quote"] = self.pair.base
            else:
                orderbook_data["ticker_id"] = self.pair.as_str
                orderbook_data["base"] = self.pair.base
                orderbook_data["quote"] = self.pair.quote

            orderbook_data["timestamp"] = f"{int(time.time())}"
            data = self.get_and_parse()

            orderbook_data["bids"] = data["bids"][:depth][::-1]
            orderbook_data["asks"] = data["asks"][::-1][:depth]
            total_bids_base_vol = sum(
                [Decimal(i["volume"]) for i in orderbook_data["bids"]]
            )
            total_asks_base_vol = sum(
                [Decimal(i["volume"]) for i in orderbook_data["asks"]]
            )
            total_bids_quote_vol = sum(
                [
                    Decimal(i["volume"]) * Decimal(i["price"])
                    for i in orderbook_data["bids"]
                ]
            )
            total_asks_quote_vol = sum(
                [
                    Decimal(i["volume"]) * Decimal(i["price"])
                    for i in orderbook_data["asks"]
                ]
            )
            orderbook_data["base_price_usd"] = lib.get_gecko_price(
                orderbook_data["base"], self.gecko_source
            )
            orderbook_data["quote_price_usd"] = lib.get_gecko_price(
                orderbook_data["quote"], self.gecko_source
            )
            orderbook_data["total_asks_base_vol"] = total_asks_base_vol
            orderbook_data["total_bids_base_vol"] = total_bids_base_vol
            orderbook_data["total_asks_quote_vol"] = total_asks_quote_vol
            orderbook_data["total_bids_quote_vol"] = total_bids_quote_vol
            orderbook_data["total_asks_base_usd"] = (
                total_asks_base_vol * orderbook_data["base_price_usd"]
            )
            orderbook_data["total_bids_quote_usd"] = (
                total_bids_quote_vol * orderbook_data["quote_price_usd"]
            )

            orderbook_data["liquidity_usd"] = (
                orderbook_data["total_asks_base_usd"]
                + orderbook_data["total_bids_quote_usd"]
            )
            pair = orderbook_data["ticker_id"]
            msg = f"orderbook.for_pair {pair} | netid {self.netid} ok!"
            return default_result(data=orderbook_data, msg=msg)
        except Exception as e:  # pragma: no cover
            pair = orderbook_data["ticker_id"]
            msg = f"orderbook.for_pair {pair} failed | netid {self.netid}: {e}!"
            return default_error(e, msg)

    @timed
    def get_and_parse(self):
        try:
            base = self.pair.base
            quote = self.pair.quote
            # Handle segwit only coins
            if self.base_is_segwit_coin:
                base = f"{self.pair.base.replace('-segwit', '')}-segwit"
            if self.quote_is_segwit_coin:
                quote = f"{self.pair.quote.replace('-segwit', '')}-segwit"
            data = self.orderbook_template
            if not validate_orderbook_pair(base, quote, self.coins_config):
                return data
            if self.pair.inverse_requested:
                x = self.dexapi.orderbook_rpc(quote, base)
            else:
                x = self.dexapi.orderbook_rpc(base, quote)
            for i in ["asks", "bids"]:
                items = [
                    {
                        "price": j["price"]["decimal"],
                        "volume": j["base_max_volume"]["decimal"],
                    }
                    for j in x[i]
                ]
                x[i] = items
                data[i] += x[i]
        except Exception as e:  # pragma: no cover
            msg = f"orderbook.get_and_parse {self.pair.as_str} failed | netid {self.netid}: {e}"
            return default_error(e, msg)
        msg = f"orderbook.get_and_parse {self.pair.as_str} netid {self.netid} ok!"
        return default_result(data=data, msg=msg)

    @timed
    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        try:
            if len(orderbook["asks"]) > 0:
                return format_10f(
                    min([Decimal(ask["price"]) for ask in orderbook["asks"]])
                )
        except KeyError as e:  # pragma: no cover
            return default_error(e, data=format_10f(0))
        except Exception as e:  # pragma: no cover
            return default_error(e, data=format_10f(0))
        return format_10f(0)

    @timed
    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        try:
            if len(orderbook["bids"]) > 0:
                return format_10f(
                    max([Decimal(bid["price"]) for bid in orderbook["bids"]])
                )
        except KeyError as e:  # pragma: no cover
            return default_error(e, data=format_10f(0))
        except Exception as e:  # pragma: no cover
            return default_error(e, data=format_10f(0))
        return format_10f(0)
