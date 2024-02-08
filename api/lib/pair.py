#!/usr/bin/env python3
from util.cron import cron
from collections import OrderedDict
from decimal import Decimal
from typing import Optional, List, Dict
import db.sqldb as db
from util.logger import logger, timed
from util.transform import (
    sortdata,
    clean,
    deplatform,
    sumdata,
    merge,
    invert,
    filterdata,
)
import util.defaults as default
import lib.dex_api as dex
import util.memcache as memcache
from util.transform import template
import util.transform as transform
from util.transform import derive


class Pair:  # pragma: no cover
    """
    Allows for referencing pairs as a string or tuple.
    e.g. DOGE_BTC, not BTC_DOGE
    """

    def __init__(
        self,
        pair_str: str = "KMD_LTC",
        pairs_last_trade_cache: Dict | None = None,
        coins_config: Dict | None = None,
        gecko_source: Dict | None = None,
    ):
        try:
            self.as_str = pair_str
            self.as_std_str = deplatform.pair(self.as_str)
            self.base, self.quote = derive.base_quote(self.as_str)
            self.is_reversed = self.as_str != sortdata.pair_by_market_cap(self.as_str)

            # Load standard memcache
            self.pairs_last_trade_cache = pairs_last_trade_cache
            if self.pairs_last_trade_cache is None:
                self.pairs_last_trade_cache = memcache.get_pairs_last_traded()

            self.coins_config = coins_config
            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()

            self.gecko_source = gecko_source
            if self.gecko_source is None:
                self.gecko_source = memcache.get_gecko_source()

            # Get price and market cap
            self.base_usd_price = derive.gecko_price(
                self.base, gecko_source=self.gecko_source
            )
            self.quote_usd_price = derive.gecko_price(
                self.quote, gecko_source=self.gecko_source
            )
            if self.quote_usd_price == 0 or self.base_usd_price == 0:
                self.priced = False
            else:
                self.priced = True

            # Load Database
            self.pg_query = db.SqlQuery()

        except Exception as e:  # pragma: no cover
            msg = f"Init Pair for {pair_str} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @timed
    def historical_trades(
        self,
        limit: int = 100,
        start_time: Optional[int] = 0,
        end_time: Optional[int] = 0,
    ):
        """Returns trades for this pair."""
        # TODO: Review price / reverse price logic
        try:
            if start_time == 0:
                start_time = int(cron.now_utc()) - 86400
            if end_time == 0:
                end_time = int(cron.now_utc())

            resp = {}
            swaps_for_pair = self.pg_query.get_swaps(
                start_time=start_time,
                end_time=end_time,
                pair=self.as_str,
            )
            for variant in swaps_for_pair:
                trades_info = []
                for swap in swaps_for_pair[variant]:
                    trade_info = OrderedDict()
                    trade_info["trade_id"] = swap["uuid"]
                    trade_info["timestamp"] = swap["finished_at"]
                    # Handle reversed pair requested.
                    # inverts base / quote and trade type
                    # so calcs after are correctly assigned
                    if self.is_reversed:
                        pair_str = invert.pair(swap["pair"])
                        price = Decimal(swap["reverse_price"])
                        trade_info["pair"] = pair_str
                        trade_info["type"] = invert.trade_type(swap["trade_type"])
                    else:
                        pair_str = swap["pair"]
                        price = Decimal(swap["price"])
                        trade_info["pair"] = pair_str
                        trade_info["type"] = swap["trade_type"]
                    base, quote = derive.base_quote(pair_str)
                    trade_info["price"] = transform.format_10f(price)
                    trade_info["base_coin"] = base
                    trade_info["quote_coin"] = quote
                    trade_info["base_coin_ticker"] = deplatform.coin(base)
                    trade_info["quote_coin_ticker"] = deplatform.coin(quote)
                    trade_info["base_coin_platform"] = transform.get_coin_platform(base)
                    trade_info["quote_coin_platform"] = transform.get_coin_platform(
                        quote
                    )

                    if trade_info["type"] == "buy":
                        trade_info["base_volume"] = transform.format_10f(
                            swap["maker_amount"]
                        )
                        trade_info["quote_volume"] = transform.format_10f(
                            swap["taker_amount"]
                        )
                    else:
                        trade_info["base_volume"] = transform.format_10f(
                            swap["taker_amount"]
                        )
                        trade_info["quote_volume"] = transform.format_10f(
                            swap["maker_amount"]
                        )

                    trades_info.append(trade_info)

                buys = filterdata.dict_lists(trades_info, "type", "buy")
                sells = filterdata.dict_lists(trades_info, "type", "sell")
                if len(buys) > 0:
                    buys = sortdata.dict_lists(buys, "timestamp", reverse=True)
                if len(sells) > 0:
                    sells = sortdata.dict_lists(sells, "timestamp", reverse=True)
                if variant == "ALL":
                    pair_str = deplatform.pair(pair_str)
                else:
                    pair_str = variant
                data = {
                    "ticker_id": pair_str,
                    "start_time": str(start_time),
                    "end_time": str(end_time),
                    "limit": str(limit),
                    "trades_count": str(len(trades_info)),
                    "sum_base_volume_buys": sumdata.json_key_10f(buys, "base_volume"),
                    "sum_base_volume_sells": sumdata.json_key_10f(sells, "base_volume"),
                    "sum_quote_volume_buys": sumdata.json_key_10f(buys, "quote_volume"),
                    "sum_quote_volume_sells": sumdata.json_key_10f(
                        sells, "quote_volume"
                    ),
                    "average_price": transform.format_10f(
                        self.get_average_price(trades_info)
                    ),
                    "buy": buys,
                    "sell": sells,
                }

                resp.update({variant: data})
        except Exception as e:  # pragma: no cover
            return default.result(
                data=resp,
                msg=f"pair.historical_trades {self.as_str} failed! {e}",
                loglevel="warning",
            )
        return default.result(
            data=resp,
            msg=f"pair.historical_trades for {self.as_str} complete",
            loglevel="pair",
            ignore_until=2,
        )

    @timed
    def get_average_price(self, trades_info):
        try:
            data = 0
            if len(trades_info) > 0:
                data = sumdata.json_key(trades_info, "price") / len(trades_info)
            return default.result(
                data=data,
                msg=f"get_average_price for {self.as_str} complete",
                loglevel="pair",
                ignore_until=2,
            )
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} get_average_price failed! {e}"
            return default.error(e, msg)

    @timed
    def get_prices(self, days: int = 1, all_variants: bool = True):
        """
        Iterates over list of swaps to get prices data
        """
        try:
            suffix = transform.get_suffix(days)
            data = template.prices(suffix, base=self.base, quote=self.quote)
            swaps_for_pair_combo = self.pg_query.get_swaps(
                start_time=int(cron.now_utc() - 86400 * days),
                end_time=int(cron.now_utc()),
                pair=self.as_str,
            )
            # Extract all variant swaps, or for a single variant
            key = "prices"
            cache_name = derive.pair_cachename(key, self.as_str, suffix, all_variants)
            if all_variants:
                variants = derive.pair_variants(self.as_str)
            elif self.as_str in swaps_for_pair_combo:
                variants = derive.pair_variants(self.as_str, segwit_only=True)
            elif invert.pair(self.as_str) in swaps_for_pair_combo:
                cache_name = derive.pair_cachename(
                    key, invert.pair(self.as_str), suffix
                )
                variants = derive.pair_variants(
                    invert.pair(self.as_str), segwit_only=True
                )
            else:
                logger.warning(
                    f"{self.as_str} not in swaps_for_pair_combo, returning template"
                )
                return data

            data.update(
                {
                    "base_price_usd": self.base_usd_price,
                    "quote_price_usd": self.quote_usd_price,
                    "variants": variants,
                    "cache_name": cache_name,
                }
            )
            if all_variants:
                swaps_for_pair = swaps_for_pair_combo["ALL"]
                swap_prices = self.get_swap_prices(swaps_for_pair)
            else:
                swap_prices = {}
                for variant in variants:
                    if variant in swaps_for_pair_combo:
                        swaps_for_pair = swaps_for_pair_combo[variant]
                    else:
                        logger.warning(f"Variant {variant} not in swaps for pair!")
                        continue
                    swap_prices.update(self.get_swap_prices(swaps_for_pair))

            # Get Prices
            # TODO: using timestamps as an index works for now,
            # but breaks when two swaps have the same timestamp.
            if len(swap_prices) > 0:
                swap_vals = list(swap_prices.values())
                swap_keys = list(swap_prices.keys())
                highest_price = max(swap_vals)
                lowest_price = min(swap_vals)
                newest_price = swap_prices[max(swap_prices.keys())]
                oldest_price = swap_prices[min(swap_prices.keys())]
                data["oldest_price_time"] = swap_keys[swap_vals.index(oldest_price)]
                data["newest_price_time"] = swap_keys[swap_vals.index(newest_price)]
                data["oldest_price"] = oldest_price
                data["newest_price"] = newest_price
                price_change = newest_price - oldest_price
                pct_change = newest_price / oldest_price - 1
                data[f"highest_price_{suffix}"] = highest_price
                data[f"lowest_price_{suffix}"] = lowest_price
                data[f"price_change_pct_{suffix}"] = pct_change
                data[f"price_change_{suffix}"] = price_change
                last_swap = self.first_last_swap(data["variants"])
                data["last_swap_price"] = last_swap["last_swap_price"]
                data["last_swap_time"] = last_swap["last_swap_time"]
                data["last_swap_uuid"] = last_swap["last_swap_uuid"]

            msg = f"get_prices for {self.as_str} complete!"
            return default.result(data=data, msg=msg, loglevel="cached", ignore_until=3)
        except Exception as e:  # pragma: no cover
            msg = f"get_prices for {self.as_str} failed! {e}, returning template"
            return default.result(
                data=data, msg=msg, loglevel="warning", ignore_until=0
            )

    @timed
    def first_last_swap(self, variants: List):
        try:
            data = template.first_last_swap()
            for variant in variants:
                x = template.first_last_swap()

                if variant in self.pairs_last_trade_cache:
                    x = self.pairs_last_trade_cache[variant]
                elif invert.pair(variant) in self.pairs_last_trade_cache:  # pragma: no cover
                    x = self.pairs_last_trade_cache[invert.pair(variant)]

                if x["last_swap_time"] > data["last_swap_time"]:
                    data["last_swap_time"] = x["last_swap_time"]
                    data["last_swap_price"] = x["last_swap_price"]
                    data["last_swap_uuid"] = x["last_swap_uuid"]
                    if self.is_reversed and data["last_swap_price"] != 0:
                        data["last_swap_price"] = 1 / data["last_swap_price"]

            msg = f"Got first and last swap for {self.as_str}"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
        except Exception as e:  # pragma: no cover
            data = template.first_last_swap()
            msg = f"Returning template for {self.as_str} ({e})"
            return default.result(
                data=data, msg=msg, loglevel="warning", ignore_until=0
            )

    @timed
    def ticker_info(self, days=1, all_variants: bool = False):
        # TODO: ps: in order for CoinGecko to show +2/-2% depth,
        # DEX has to provide the formula for +2/-2% depth.
        try:
            suffix = transform.get_suffix(days)
            key = "ticker_info"
            cache_name = derive.pair_cachename(key, self.as_str, suffix, all_variants)
            data = memcache.get(cache_name)
            if data is not None and Decimal(data["liquidity_in_usd"]) > 0:
                msg = f"Using cache: {cache_name}"
                return default.result(
                    data=data, msg=msg, loglevel="pair", ignore_until=3
                )
            data = template.ticker_info(suffix, self.base, self.quote)
            data.update(
                {
                    "ticker_id": self.as_str,
                    "base_currency": self.base,
                    "quote_currency": self.quote,
                    "base_usd_price": self.base_usd_price,
                    "quote_usd_price": self.quote_usd_price,
                    "priced": self.priced,
                }
            )
            data.update(self.get_prices(days, all_variants=all_variants))
            orderbook_data = self.orderbook(
                self.as_str, depth=100, all_variants=all_variants, no_thread=False
            )
            for i in orderbook_data.keys():
                if i in ["bids", "asks"]:
                    data.update({f"num_{i}": len(orderbook_data[i])})
                else:
                    data.update({i: orderbook_data[i]})

            ignore_until = 3
            loglevel = "pair"
            msg = f"ticker_info for {self.as_str} ({days} days) complete!"
            # Add to cache if fully populated
            if Decimal(data["liquidity_in_usd"]) > 0:
                segwit_variants = derive.pair_variants(self.as_str, segwit_only=True)
                for sv in segwit_variants:
                    cache_name = derive.pair_cachename(key, sv, suffix, all_variants)
                    base, quote = derive.base_quote(sv)
                    data.update(
                        {
                            "ticker_id": sv,
                            "pair": sv,
                            "base": base,
                            "quote": quote,
                            "ticker_info_cache_name": cache_name,
                        }
                    )
                    data = clean.decimal_dicts(data)
                    memcache.update(cache_name, data, 900)
                    msg = f" Added to memcache [{cache_name}]"
                    if Decimal(data["liquidity_in_usd"]) > 10000:
                        msg = f'[{cache_name}] liquidity {data["liquidity_in_usd"]}'
                        ignore_until = 5

        except Exception as e:  # pragma: no cover
            ignore_until = 0
            loglevel = "warning"
            msg = f"ticker_info for {self.as_str} ({days} days) failed! {e}"
        return default.result(
            data=data, msg=msg, loglevel=loglevel, ignore_until=ignore_until
        )

    @timed
    def get_swap_prices(self, swaps_for_pair):
        try:
            data = {}
            [
                data.update(derive.price_at_finish(i, is_reverse=self.is_reversed))
                for i in swaps_for_pair
            ]
        except Exception as e:  # pragma: no cover
            msg = f"get_swap_prices for {self.as_str} failed! {e}"
            return default.result(data=data, msg=msg, loglevel="warning")
        msg = f"Completed get_swap_prices info for {self.as_str}"
        return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

    @timed
    def swap_uuids(
        self,
        start_time: Optional[int] = 0,
        end_time: Optional[int] = 0,
        all_variants: bool = True,
    ) -> list:
        try:
            data = self.pg_query.swap_uuids(
                start_time=start_time, end_time=end_time, pair=self.as_str
            )
            if all_variants:
                variants = sorted([i for i in data.keys() if i != "ALL"])
                data = {"uuids": data["ALL"], "variants": variants}
            elif self.as_str in data:
                data = {"uuids": data[self.as_str], "variants": [self.as_str]}
            elif invert.pair(self.as_str) in data:
                data = {
                    "uuids": data[invert.pair(self.as_str)],
                    "variants": [invert.pair(self.as_str)],
                }

        except Exception as e:  # pragma: no cover
            data = {"uuids": [], "variants": [self.as_str]}
            msg = f"{self.as_str} swap_uuids failed! {e}"
            return default.result(data=data, msg=msg, loglevel="warning")
        msg = f"Completed swap_uuids for {self.as_str}"
        return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

    @timed
    def orderbook(
        self,
        pair_str: str = "KMD_LTC",
        depth: int = 100,
        all_variants: bool = False,
        no_thread: bool = True,
    ):
        try:
            if all_variants:
                pair_str = deplatform.pair(pair_str)
                pair_tpl = derive.base_quote(pair_str)
                combo_cache_name = f"orderbook_{pair_str}_ALL"
                variants = derive.pair_variants(pair_str)
            else:
                # This will be a single ticker_pair unless for segwit
                pair_tpl = derive.base_quote(pair_str)
                combo_cache_name = f"orderbook_{pair_str}"
                variants = [pair_str]
            if len(pair_tpl) != 2 or "error" in pair_tpl:
                return {"error": "Market pair should be in `KMD_BTC` format"}
            combined_orderbook = template.orderbook(pair_str)
            combined_orderbook.update({"variants": variants})

            # Use combined cache if valid
            combo_orderbook_cache = memcache.get(combo_cache_name)
            if combo_orderbook_cache is not None and memcache.get("testing") is None:
                if (
                    len(combo_orderbook_cache["asks"]) > 0
                    and len(combo_orderbook_cache["bids"]) > 0
                ):
                    combined_orderbook = combo_orderbook_cache
            else:
                if self.coins_config is None:
                    self.coins_config = memcache.get_coins_config()

                if self.gecko_source is None:
                    self.gecko_source = memcache.get_gecko_source()

                for variant in variants:
                    variant_cache_name = f"orderbook_{variant}"
                    base, quote = derive.base_quote(variant)
                    data = dex.get_orderbook(
                        base=base,
                        quote=quote,
                        coins_config=self.coins_config,
                        gecko_source=self.gecko_source,
                        variant_cache_name=variant_cache_name,
                        depth=depth,
                        no_thread=no_thread,
                    )
                    # Apply depth limit after caching so cache is complete
                    data["bids"] = data["bids"][:depth][::-1]
                    data["asks"] = data["asks"][::-1][:depth]
                    # TODO: Recalc liquidity if depth is less than data.
                    # Merge with other variants

                    if all_variants:
                        # Avoid double counting on segwit variants
                        if "segwit" in variant:
                            base_variants = derive.coin_variants(base)
                            if (
                                deplatform.coin(base) in base_variants
                                and "-segwit" in base
                            ):
                                continue
                            quote_variants = derive.coin_variants(quote)
                            if (
                                deplatform.coin(quote) in quote_variants
                                and "-segwit" in quote
                            ):
                                continue
                            combined_orderbook = merge.orderbooks(
                                combined_orderbook, data
                            )
                        else:
                            combined_orderbook = merge.orderbooks(
                                combined_orderbook, data
                            )
                    else:
                        combined_orderbook = merge.orderbooks(combined_orderbook, data)
                # Sort variant bids / asks
                combined_orderbook["bids"] = combined_orderbook["bids"][: int(depth)][
                    ::-1
                ]
                combined_orderbook["asks"] = combined_orderbook["asks"][::-1][
                    : int(depth)
                ]
                combined_orderbook = clean.orderbook_data(combined_orderbook)
                combined_orderbook["pair"] = pair_str
                combined_orderbook["base"] = pair_tpl[0]
                combined_orderbook["quote"] = pair_tpl[1]
                # update the combined cache
                if (
                    len(combined_orderbook["asks"]) > 0
                    or len(combined_orderbook["bids"]) > 0
                ):
                    data = clean.decimal_dicts(data)
                    dex.add_orderbook_to_cache(pair_str, combo_cache_name, data)

            msg = f"[{combo_cache_name}] ${combined_orderbook['liquidity_in_usd']} liquidity"
            # msg += f" Variants: ({variants})"
            ignore_until = 3
            if Decimal(combined_orderbook["liquidity_in_usd"]) > 10000:
                ignore_until = 0
            return default.result(
                data=combined_orderbook,
                msg=msg,
                loglevel="cached",
                ignore_until=ignore_until,
            )
        except Exception as e:  # pragma: no cover
            msg = f"Pair.orderbook {pair_str} failed: {e}!"
            try:
                data = template.orderbook(pair_str)
                msg += " Returning template!"
            except Exception as e:  # pragma: no cover
                data = {"error": f"{msg}: {e}"}
            return default.result(
                data=data, msg=msg, loglevel="warning", ignore_until=0
            )
