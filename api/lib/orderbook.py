#!/usr/bin/env python3

from collections import OrderedDict
from decimal import Decimal
from typing import Dict
import db
import lib
from util.files import Files
from lib.coins import get_gecko_price, get_segwit_coins
from util.logger import logger, timed
from util.transform import merge, sortdata
import util.cron as cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform


class Orderbook:
    def __init__(self, pair_obj, coins_config: Dict | None = None, **kwargs):
        try:
            self.kwargs = kwargs
            self.pair = pair_obj
            self.base = self.pair.base
            self.quote = self.pair.quote
            self.options = ["mm2_host"]
            default.params(self, self.kwargs, self.options)
            self.coins_config = coins_config
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()

            self.pg_query = db.SqlQuery()
            self.files = Files(**kwargs)
            segwit_coins = [i for i in get_segwit_coins()]
            self.base_is_segwit_coin = self.base in segwit_coins
            self.quote_is_segwit_coin = self.quote in segwit_coins

        except Exception as e:  # pragma: no cover
            logger.error({"error": f"{type(e)} Failed to init Orderbook: {e}"})

    @timed
    def for_pair(self, depth=100, all: bool = False):
        if all:
            variants = helper.get_pair_variants(self.pair.as_str)
            combined_orderbook = template.orderbook(
                transform.strip_pair_platforms(self.pair.as_str)
            )
        else:
            # Segwit / non-segwit should always be merged
            variants = helper.get_pair_variants(self.pair.as_str, segwit_only=True)
            combined_orderbook = template.orderbook(self.pair.as_str)
        try:
            for variant in variants:
                orderbook_data = OrderedDict()
                orderbook_data["ticker_id"] = self.pair.as_str
                orderbook_data["base"] = self.base
                orderbook_data["quote"] = self.quote

                orderbook_data["timestamp"] = f"{int(cron.now_utc())}"
                base, quote = helper.base_quote_from_pair(variant)
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
                    orderbook_data["base"]
                )
                orderbook_data["quote_price_usd"] = get_gecko_price(
                    orderbook_data["quote"]
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
                combined_orderbook = merge.orderbooks(
                    combined_orderbook, orderbook_data
                )

            msg = f"orderbook.for_pair {self.pair.as_str} ({len(variants)} variants) complete!"
            return default.result(
                data=combined_orderbook, msg=msg, loglevel="pair", ignore_until=2
            )
        except Exception as e:  # pragma: no cover
            msg = f"orderbook.for_pair {self.pair.as_str} ({len(variants)} variants)"
            msg += f" failed: {e}! Returning template!"
            return default.result(data=combined_orderbook, msg=msg, loglevel="pair")

    @timed
    def get_and_parse(self, base: str | None = None, quote: str | None = None):
        if base is None:
            base = self.base
        if quote is None:
            quote = self.quote
        data = template.orderbook(f"{base}_{quote}")
        try:
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
            if base not in self.coins_config or quote not in self.coins_config:
                pass
            elif self.coins_config[base]["wallet_only"]:
                pass
            elif self.coins_config[quote]["wallet_only"]:
                pass
            else:
                cached = memcache.get(f"orderbook_{base}_{quote}")
                if cached is None:
                    t = lib.OrderbookRpcThread(base, quote)
                    t.start()
                else:
                    data = cached
            msg = f"orderbook.for_pair {base}_{quote} complete!"
        except Exception as e:  # pragma: no cover
            msg = f"orderbook.for_pair {base}_{quote} failed: {e}! Returning template!"
        return default.result(data=data, msg=msg, loglevel="muted")


    # The lowest ask / highest bid needs to be inverted
    # to result in conventional vaules like seen at 
    # https://api.binance.com/api/v1/ticker/24hr where
    # askPrice > bidPrice
    @timed
    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        try:
            if len(orderbook["bids"]) > 0:
                return transform.format_10f(
                    min([Decimal(bid["price"]) for bid in orderbook["bids"]])
                )
        except KeyError as e:  # pragma: no cover
            return default.error(e, data=transform.format_10f(0))
        except Exception as e:  # pragma: no cover
            return default.error(e, data=transform.format_10f(0))
        return transform.format_10f(0)

    @timed
    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        try:
            if len(orderbook["asks"]) > 0:
                return transform.format_10f(
                    max([Decimal(ask["price"]) for ask in orderbook["asks"]])
                )
        except KeyError as e:  # pragma: no cover
            return default.error(e, data=transform.format_10f(0))
        except Exception as e:  # pragma: no cover
            return default.error(e, data=transform.format_10f(0))
        return transform.format_10f(0)
