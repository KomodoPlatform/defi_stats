#!/usr/bin/env python3
import util.cron as cron
from collections import OrderedDict
from decimal import Decimal
from util.files import Files
from util.logger import logger, timed
from util.defaults import default_error, set_params, default_result
from util.helper import get_gecko_price, get_pair_variants
import lib
import db
import util.transform as transform
import util.templates as template


class Orderbook:
    def __init__(self, pair_obj, **kwargs):
        try:
            self.kwargs = kwargs
            self.pair = pair_obj
            self.base = self.pair.base
            self.quote = self.pair.quote
            self.options = ["mm2_host"]
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
                self.last_traded_cache = lib.load_generic_last_traded()

            self.pg_query = db.SqlQuery(
                gecko_source=self.gecko_source,
                coins_config=self.coins_config
            )
            self.files = Files(**kwargs)
            segwit_coins = [i.coin for i in lib.COINS.with_segwit]
            self.base_is_segwit_coin = self.base in segwit_coins
            self.quote_is_segwit_coin = self.quote in segwit_coins

        except Exception as e:  # pragma: no cover
            logger.error({"error": f"{type(e)} Failed to init Orderbook: {e}"})

    @timed
    def for_pair(self, depth=100, all: bool = False):
        try:
            if all:
                variants = get_pair_variants(
                    self.pair.as_str, coins_config=self.coins_config
                )
                combined_orderbook = template.orderbook(
                    transform.strip_pair_platforms(self.pair.as_str)
                )
            else:
                variants = [self.pair.as_str]
                combined_orderbook = template.orderbook(self.pair.as_str)

            for variant in variants:
                orderbook_data = OrderedDict()
                orderbook_data["ticker_id"] = self.pair.as_str
                orderbook_data["base"] = self.base
                orderbook_data["quote"] = self.quote

                orderbook_data["timestamp"] = f"{int(cron.now_utc())}"
                base, quote = transform.base_quote_from_pair(variant)
                if self.base_is_segwit_coin and len(variants) > 1:
                    if "-" not in base:
                        continue

                if self.quote_is_segwit_coin and len(variants) > 1:
                    if "-" not in quote:
                        continue

                data = self.get_and_parse(base=base, quote=quote)

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
                orderbook_data["base_price_usd"] = get_gecko_price(
                    orderbook_data["base"], self.gecko_source
                )
                orderbook_data["quote_price_usd"] = get_gecko_price(
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
                msg = f"orderbook.for_pair {pair} ok!"

                combined_orderbook = transform.merge_orderbooks(
                    combined_orderbook, orderbook_data
                )

            msg = "Got orderbook for pair"
            return default_result(data=combined_orderbook, msg=msg, loglevel="muted")
        except Exception as e:  # pragma: no cover
            pair = orderbook_data["ticker_id"]
            msg = f"orderbook.for_pair {pair} failed: {e}!"
            return default_error(e, msg)

    @timed
    def get_and_parse(self, base: str | None = None, quote: str | None = None):
        if base is None:
            base = self.base
        if quote is None:
            quote = self.quote
        if base not in self.coins_config:
            return template.orderbook(f"{base}_{quote}")
        elif self.coins_config[base]["wallet_only"]:
            return template.orderbook(f"{base}_{quote}")
        if quote not in self.coins_config:
            return template.orderbook(f"{base}_{quote}")
        elif self.coins_config[quote]["wallet_only"]:
            return template.orderbook(f"{base}_{quote}")
        return lib.get_orderbook(base, quote)

    @timed
    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        try:
            if len(orderbook["asks"]) > 0:
                return transform.format_10f(
                    min([Decimal(ask["price"]) for ask in orderbook["asks"]])
                )
        except KeyError as e:  # pragma: no cover
            return default_error(e, data=transform.format_10f(0))
        except Exception as e:  # pragma: no cover
            return default_error(e, data=transform.format_10f(0))
        return transform.format_10f(0)

    @timed
    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        try:
            if len(orderbook["bids"]) > 0:
                return transform.format_10f(
                    max([Decimal(bid["price"]) for bid in orderbook["bids"]])
                )
        except KeyError as e:  # pragma: no cover
            return default_error(e, data=transform.format_10f(0))
        except Exception as e:  # pragma: no cover
            return default_error(e, data=transform.format_10f(0))
        return transform.format_10f(0)
