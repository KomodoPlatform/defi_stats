#!/usr/bin/env python3
import util.cron as cron
from collections import OrderedDict
from decimal import Decimal
from typing import Optional, List
import db
from util.transform import (
    sum_json_key,
    sum_json_key_10f,
    sort_dict_list,
    format_10f,
    list_json_key,
    get_suffix,
    invert_pair,
)

from util.defaults import default_error, set_params, default_result
from util.enums import TradeType
from util.helper import (
    get_price_at_finish,
    get_last_trade_item,
    get_gecko_price,
    get_gecko_mcap,
    get_swaps_volumes,
)
from util.logger import logger, timed
import util.templates as template
import util.transform as transform
import lib


class Pair:  # pragma: no cover
    """
    Allows for referencing pairs as a string or tuple.
    e.g. DOGE_BTC, not BTC_DOGE
    """

    def __init__(self, pair_str: str, **kwargs):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = []
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
                gecko_source=self.gecko_source, coins_config=self.coins_config
            )
            # Adjust pair order
            self.as_str = pair_str
            self.as_std_str = transform.strip_pair_platforms(self.as_str)
            self.is_reversed = self.as_str != transform.order_pair_by_market_cap(
                self.as_str, gecko_source=self.gecko_source
            )

            self.base = self.as_str.split("_")[0]
            self.quote = self.as_str.split("_")[1]
            self.as_tuple = tuple((self.base, self.quote))
            self.as_set = set((self.base, self.quote))

            # Get price and market cap
            self.base_usd_price = get_gecko_price(self.base, self.gecko_source)
            self.quote_usd_price = get_gecko_price(self.quote, self.gecko_source)
            self.base_mcap = get_gecko_mcap(self.base, self.gecko_source)
            self.quote_mcap = get_gecko_mcap(self.quote, self.gecko_source)

        except Exception as e:  # pragma: no cover
            msg = f"Init Pair for {pair_str} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def is_tradable(self):
        if len(set(lib.COINS.tradable_only).intersection(self.as_set)) != 2:
            return True
        return False  # pragma: no cover

    @property
    def is_priced(self):
        if self.base_usd_price > 0 and self.quote_usd_price > 0:
            return True
        return False

    @property
    def info(self):
        data = template.pair_info(f"{self.base}_{self.quote}")
        last_trade = get_last_trade_item(
            self.as_str, self.last_traded_cache, "last_swap"
        )
        data.update({"last_trade": last_trade})
        return data

    @property
    def orderbook(self):
        # Handles reverse pairs
        return lib.Orderbook(
            pair_obj=self,
            gecko_source=self.gecko_source,
            last_traded_cache=self.last_traded_cache,
        )

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
                    # Handle reversed pair
                    if self.is_reversed:
                        price = Decimal(swap["reverse_price"])
                        trade_info["pair"] = transform.invert_pair(swap["pair"])
                        trade_info["type"] = transform.invert_trade_type(
                            swap["trade_type"]
                        )
                    else:
                        price = Decimal(swap["price"])
                        trade_info["pair"] = swap["pair"]
                        trade_info["type"] = swap["trade_type"]

                    if trade_info["type"] == "buy":
                        trade_info["base_volume"] = format_10f(swap["maker_amount"])
                        trade_info["quote_volume"] = format_10f(swap["taker_amount"])
                        trade_info["target_volume"] = format_10f(swap["taker_amount"])
                    else:
                        trade_info["base_volume"] = format_10f(swap["taker_amount"])
                        trade_info["quote_volume"] = format_10f(swap["maker_amount"])
                        trade_info["target_volume"] = format_10f(swap["maker_amount"])

                    trade_info["price"] = format_10f(price)
                    trades_info.append(trade_info)

                average_price = self.get_average_price(trades_info)
                buys = list_json_key(trades_info, "type", "buy")
                sells = list_json_key(trades_info, "type", "sell")
                if len(buys) > 0:
                    buys = sort_dict_list(buys, "timestamp", reverse=True)
                if len(sells) > 0:
                    sells = sort_dict_list(sells, "timestamp", reverse=True)

                data = {
                    "ticker_id": self.as_str,
                    "start_time": str(start_time),
                    "end_time": str(end_time),
                    "limit": str(limit),
                    "trades_count": str(len(trades_info)),
                    "sum_base_volume_buys": sum_json_key_10f(buys, "base_volume"),
                    "sum_base_volume_sells": sum_json_key_10f(sells, "base_volume"),
                    "sum_target_volume_buys": sum_json_key_10f(buys, "target_volume"),
                    "sum_target_volume_sells": sum_json_key_10f(sells, "target_volume"),
                    "sum_quote_volume_buys": sum_json_key_10f(buys, "quote_volume"),
                    "sum_quote_volume_sells": sum_json_key_10f(sells, "quote_volume"),
                    "average_price": format_10f(average_price),
                    "buy": buys,
                    "sell": sells,
                }
                resp.update({variant: data})

        except Exception as e:  # pragma: no cover
            msg = f"pair.historical_trades {self.as_str} failed!"
            return default_error(e, msg)

        return default_result(data=resp, msg="historical_trades complete")

    @timed
    def get_average_price(self, trades_info):
        try:
            if len(trades_info) > 0:
                return sum_json_key(trades_info, "price") / len(trades_info)
            return 0
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} get_average_price failed!"
            return default_error(e, msg)

    @timed
    def get_volumes_and_prices(self, days: int = 1, all: bool = True):
        """
        Iterates over list of swaps to get volumes and prices data
        """
        try:
            suffix = get_suffix(days)
            data = template.volumes_and_prices(suffix)
            swaps_for_pair = self.pg_query.get_swaps(
                start_time=int(cron.now_utc() - 86400 * days),
                end_time=int(cron.now_utc()),
                pair=self.as_str,
            )

            # Extract all variant swaps, or for a single variant
            if all:
                variants = sorted([i for i in swaps_for_pair.keys() if i != "ALL"])
                data["variants"] = variants
                swaps_for_pair = swaps_for_pair["ALL"]
            elif self.as_str in swaps_for_pair:
                swaps_for_pair = swaps_for_pair[self.as_str]
                data["variants"] = [self.as_str]
            elif transform.invert_pair(self.as_str) in swaps_for_pair:
                swaps_for_pair = swaps_for_pair[transform.invert_pair(self.as_str)]
                data["variants"] = [transform.invert_pair(self.as_str)]
            else:
                swaps_for_pair = []

            data["base"] = self.base
            data["quote"] = self.quote
            data["base_price"] = self.base_usd_price
            data["quote_price"] = self.quote_usd_price
            data[f"trades_{suffix}"] = len(swaps_for_pair)

            # Get Volumes
            swaps_volumes = get_swaps_volumes(swaps_for_pair, self.is_reversed)

            data["base_volume"] = swaps_volumes[0]
            data["quote_volume"] = swaps_volumes[1]

            data["base_volume_usd"] = Decimal(swaps_volumes[0]) * Decimal(
                self.base_usd_price
            )
            data["quote_volume_usd"] = Decimal(swaps_volumes[1]) * Decimal(
                self.quote_usd_price
            )

            # Halving the combined volume to not double count, and
            # get average between base and quote
            data["combined_volume_usd"] = (
                data["base_volume_usd"] + data["quote_volume_usd"]
            ) / 2

            # Get Prices
            # TODO: using timestamps as an index works for now,
            # but breaks when two swaps have the same timestamp.
            swap_prices = self.get_swap_prices(swaps_for_pair, self.is_reversed)
            if len(swap_prices) > 0:
                swap_vals = list(swap_prices.values())
                swap_keys = list(swap_prices.keys())
                highest_price = max(swap_vals)
                lowest_price = min(swap_vals)
                newest_price = swap_prices[max(swap_prices.keys())]
                oldest_price = swap_prices[min(swap_prices.keys())]
                data["oldest_price"] = oldest_price
                data["newest_price"] = newest_price
                data["oldest_price_time"] = swap_keys[swap_vals.index(oldest_price)]
                data["newest_price_time"] = swap_keys[swap_vals.index(newest_price)]
                price_change = newest_price - oldest_price
                pct_change = newest_price / oldest_price - 1
                data[f"highest_price_{suffix}"] = highest_price
                data[f"lowest_price_{suffix}"] = lowest_price
                data[f"price_change_percent_{suffix}"] = pct_change
                data[f"price_change_{suffix}"] = price_change
                # TODO: These values are capped to last 24hrs
                # We could get it from `last traded cache`
                # for longer term coverage
                last_swap = self.first_last_swap(data["variants"])
                data["last_swap_price"] = last_swap["last_swap_price"]
                data["last_swap_time"] = last_swap["last_swap_time"]
                data["last_swap_uuid"] = last_swap["last_swap_uuid"]
                data["first_swap_price"] = last_swap["first_swap_price"]
                data["first_swap_time"] = last_swap["first_swap_time"]
                data["first_swap_uuid"] = last_swap["first_swap_uuid"]

            return data
        except Exception as e:  # pragma: no cover
            msg = f"get_volumes_and_prices for {self.as_str} failed! {e}"
            return default_error(e, msg)

    def first_last_swap(self, variants: List):
        data = template.first_last_swap()
        for variant in variants:
            x = template.first_last_swap()
            if variant in self.last_traded_cache:
                x = self.last_traded_cache[variant]
            elif invert_pair(variant) in self.last_traded_cache:  # pragma: no cover
                x = self.last_traded_cache[invert_pair(variant)]

            if x["last_swap_time"] > data["last_swap_time"]:
                data["last_swap_time"] = x["last_swap_time"]
                data["last_swap_price"] = x["last_swap_price"]
                data["last_swap_uuid"] = x["last_swap_uuid"]
                if self.is_reversed:
                    data["last_swap_price"] = 1 / data["last_swap_price"]

            if x["first_swap_time"] < data["first_swap_time"]:
                data["first_swap_time"] = x["first_swap_time"]
                data["first_swap_price"] = x["first_swap_price"]
                data["first_swap_uuid"] = x["first_swap_uuid"]
                if self.is_reversed:
                    data["first_swap_price"] = 1 / data["first_swap_price"]

        return data

    @timed
    def get_liquidity(self, orderbook_data):
        """Liquidity for pair from current orderbook & usd price."""
        try:
            base_liq_coins = Decimal(orderbook_data["total_asks_base_vol"])
            rel_liq_coins = Decimal(orderbook_data["total_bids_quote_vol"])
            base_liq_usd = Decimal(self.base_usd_price) * Decimal(base_liq_coins)
            rel_liq_usd = Decimal(self.quote_usd_price) * Decimal(rel_liq_coins)
            base_liq_usd = Decimal(base_liq_usd)
            rel_liq_usd = Decimal(rel_liq_usd)
            data = {
                "rel_usd_price": self.quote_usd_price,
                "rel_liquidity_coins": rel_liq_coins,
                "rel_liquidity_usd": rel_liq_usd,
                "base_usd_price": self.base_usd_price,
                "base_liquidity_coins": base_liq_coins,
                "base_liquidity_usd": base_liq_usd,
                "liquidity_usd": base_liq_usd + rel_liq_usd,
            }
            return data
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} failed!"
            return default_error(e, msg)

    @timed
    def ticker_info(self, days=1, all: bool = False):
        # TODO: ps: in order for CoinGecko to show +2/-2% depth,
        # DEX has to provide the formula for +2/-2% depth.
        try:
            suffix = get_suffix(days)
            vol_price_data = self.get_volumes_and_prices(days, all=all)
            orderbook_data = self.orderbook.for_pair(depth=100, all=all)
            liquidity = self.get_liquidity(orderbook_data)
            resp = {
                "ticker_id": self.as_str,
                "pool_id": self.as_str,
                "variants": vol_price_data["variants"],
                f"trades_{suffix}": f'{vol_price_data[f"trades_{suffix}"]}',
                "base_currency": self.base,
                "base_volume": vol_price_data["base_volume"],
                "base_usd_price": self.base_usd_price,
                "target_currency": self.quote,
                "target_volume": vol_price_data["quote_volume"],
                "target_usd_price": self.quote_usd_price,
                "last_swap_price": vol_price_data["last_swap_price"],
                "last_swap_time": vol_price_data["last_swap_time"],
                "last_swap_uuid": vol_price_data["last_swap_uuid"],
                "first_swap_price": vol_price_data["first_swap_price"],
                "first_swap_time": vol_price_data["first_swap_time"],
                "first_swap_uuid": vol_price_data["first_swap_uuid"],
                "oldest_price": vol_price_data["oldest_price"],
                "newest_price": vol_price_data["newest_price"],
                "oldest_price_time": vol_price_data["oldest_price_time"],
                "newest_price_time": vol_price_data["newest_price_time"],
                "bid": self.orderbook.find_highest_bid(orderbook_data),
                "ask": self.orderbook.find_lowest_ask(orderbook_data),
                "high": format_10f(vol_price_data[f"highest_price_{suffix}"]),
                "low": format_10f(vol_price_data[f"lowest_price_{suffix}"]),
                f"volume_usd_{suffix}": format_10f(
                    vol_price_data["combined_volume_usd"]
                ),
                "base_volume_usd": format_10f(vol_price_data["base_volume_usd"]),
                "quote_volume_usd": format_10f(vol_price_data["quote_volume_usd"]),
                "liquidity_in_usd": format_10f(liquidity["liquidity_usd"]),
                "base_liquidity_coins": format_10f(liquidity["base_liquidity_coins"]),
                "base_liquidity_usd": format_10f(liquidity["base_liquidity_usd"]),
                "quote_liquidity_coins": format_10f(liquidity["rel_liquidity_coins"]),
                "quote_liquidity_usd": format_10f(liquidity["rel_liquidity_usd"]),
                f"price_change_percent_{suffix}": vol_price_data[
                    f"price_change_percent_{suffix}"
                ],
                f"price_change_{suffix}": vol_price_data[f"price_change_{suffix}"],
            }
            return resp
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} failed!"
            return default_error(e, msg)

    @timed
    def get_swap_prices(self, swaps_for_pair, is_reversed):
        try:
            data = {}
            [data.update(get_price_at_finish(i, is_reversed)) for i in swaps_for_pair]
            return data
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} failed!"
            return default_error(e, msg)

    def pair_swaps(
        self,
        limit: int = 100,
        trade_type: TradeType = TradeType.ALL,
        start_time: Optional[int] = 0,
        end_time: Optional[int] = 0,
    ):
        try:
            if start_time == 0:
                start_time = int(cron.now_utc()) - 86400
            if end_time == 0:
                end_time = int(cron.now_utc())
            data = self.pg_query.get_swaps_for_pair(
                base=self.base,
                quote=self.quote,
                limit=limit,
                trade_type=trade_type,
                start_time=start_time,
                end_time=end_time,
            )
            return data
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} pair_swaps failed!"
            return default_error(e, msg)

    @timed
    def swap_uuids(
        self,
        start_time: Optional[int] = 0,
        end_time: Optional[int] = 0,
        all=True,
    ) -> list:
        try:
            data = self.pg_query.swap_uuids(
                start_time=start_time, end_time=end_time, pair=self.as_str
            )
            if all:
                variants = sorted([i for i in data.keys() if i != "ALL"])
                return {"uuids": data["ALL"], "variants": variants}
            elif self.as_str in data:
                return {"uuids": data[self.as_str], "variants": [self.as_str]}
            elif transform.invert_pair(self.as_str) in data:
                return {
                    "uuids": data[transform.invert_pair(self.as_str)],
                    "variants": [transform.invert_pair(self.as_str)],
                }
            else:
                {"uuids": [], "variants": []}

        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} failed!"
            return default_error(e, msg)


@timed
def get_all_coin_pairs(coin, priced_coins):
    try:
        gecko_source = lib.load_gecko_source()
        pairs = [
            (f"{i}_{coin}") for i in priced_coins if coin not in [i, f"{i}-segwit"]
        ]
        sorted_pairs = set(
            [transform.order_pair_by_market_cap(i, gecko_source) for i in pairs]
        )
        return list(sorted_pairs)
    except Exception as e:  # pragma: no cover
        msg = "get_all_coin_pairs failed"
        return default_error(e, msg)
