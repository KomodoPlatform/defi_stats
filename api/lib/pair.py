#!/usr/bin/env python3
import util.cron as cron
from collections import OrderedDict
from decimal import Decimal
from typing import Optional, List, Dict
import db
import lib
from lib.coins import get_gecko_price, get_gecko_mcap, get_tradable_coins
from util.enums import TradeType
from util.logger import logger, timed
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform


class Pair:  # pragma: no cover
    """
    Allows for referencing pairs as a string or tuple.
    e.g. DOGE_BTC, not BTC_DOGE
    """

    def __init__(
        self,
        pair_str: str,
        last_traded_cache: Dict | None = None,
        coins_config: Dict | None = None,
        **kwargs,
    ):
        try:
            # Set params
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)

            self.pg_query = db.SqlQuery()
            # Adjust pair order
            self.as_str = pair_str
            self.as_std_str = transform.strip_pair_platforms(self.as_str)
            self.is_reversed = self.as_str != transform.order_pair_by_market_cap(
                self.as_str
            )
            base, quote = helper.base_quote_from_pair(self.as_str)
            self.base = base
            self.quote = quote
            self.as_tuple = tuple((self.base, self.quote))
            self.as_set = set((self.base, self.quote))

            # Get price and market cap
            self.base_usd_price = get_gecko_price(self.base)
            self.quote_usd_price = get_gecko_price(self.quote)
            self.base_mcap = get_gecko_mcap(self.base)
            self.quote_mcap = get_gecko_mcap(self.quote)
            self.last_traded_cache = last_traded_cache

            if self.last_traded_cache is None:
                self.last_traded_cache = memcache.get_last_traded()
            self.coins_config = coins_config

            if self.coins_config is None:
                self.coins_config = memcache.get_coins_config()
            self.orderbook = lib.Orderbook(
                pair_obj=self, coins_config=self.coins_config
            )

        except Exception as e:  # pragma: no cover
            msg = f"Init Pair for {pair_str} failed!"
            logger.error(f"{type(e)} {msg}: {e}")

    @property
    def is_tradable(self):
        if len(set(get_tradable_coins()).intersection(self.as_set)) != 2:
            return True
        return False  # pragma: no cover

    @property
    def is_priced(self):
        if self.base_usd_price > 0 and self.quote_usd_price > 0:
            return True
        return False

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
                        trade_info["base_volume"] = transform.format_10f(
                            swap["maker_amount"]
                        )
                        trade_info["quote_volume"] = transform.format_10f(
                            swap["taker_amount"]
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
                        trade_info["quote_volume"] = transform.format_10f(
                            swap["maker_amount"]
                        )

                    trade_info["price"] = transform.format_10f(price)
                    trades_info.append(trade_info)

                average_price = self.get_average_price(trades_info)
                buys = transform.list_json_key(trades_info, "type", "buy")
                sells = transform.list_json_key(trades_info, "type", "sell")
                if len(buys) > 0:
                    buys = transform.sort_dict_list(buys, "timestamp", reverse=True)
                if len(sells) > 0:
                    sells = transform.sort_dict_list(sells, "timestamp", reverse=True)

                data = {
                    "ticker_id": self.as_str,
                    "start_time": str(start_time),
                    "end_time": str(end_time),
                    "limit": str(limit),
                    "trades_count": str(len(trades_info)),
                    "sum_base_volume_buys": transform.sum_json_key_10f(
                        buys, "base_volume"
                    ),
                    "sum_base_volume_sells": transform.sum_json_key_10f(
                        sells, "base_volume"
                    ),
                    "sum_quote_volume_buys": transform.sum_json_key_10f(
                        buys, "quote_volume"
                    ),
                    "sum_quote_volume_sells": transform.sum_json_key_10f(
                        sells, "quote_volume"
                    ),
                    "sum_quote_volume_buys": transform.sum_json_key_10f(
                        buys, "quote_volume"
                    ),
                    "sum_quote_volume_sells": transform.sum_json_key_10f(
                        sells, "quote_volume"
                    ),
                    "average_price": transform.format_10f(average_price),
                    "buy": buys,
                    "sell": sells,
                }
                resp.update({variant: data})

        except Exception as e:  # pragma: no cover
            msg = f"pair.historical_trades {self.as_str} failed!"
            return default.error(e, msg)
        return default.result(
            data=resp,
            msg=f"historical_trades for {self.as_str} complete",
            loglevel="pair",
            ignore_until=2,
        )

    @timed
    def get_average_price(self, trades_info):
        try:
            if len(trades_info) > 0:
                data = transform.sum_json_key(trades_info, "price") / len(trades_info)
            data = 0
            return default.result(
                data=data,
                msg=f"get_average_price for {self.as_str} complete",
                loglevel="pair",
                ignore_until=2,
            )
        except Exception as e:  # pragma: no cover
            msg = f"{self.as_str} get_average_price failed!"
            return default.error(e, msg)

    @timed
    def get_volumes_and_prices(self, days: int = 1, all: bool = True):
        """
        Iterates over list of swaps to get volumes and prices data
        """
        suffix = transform.get_suffix(days)
        data = template.volumes_and_prices(suffix, base=self.base, quote=self.quote)
        try:
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
            data["base_price"] = self.base_usd_price
            data["quote_price"] = self.quote_usd_price
            data[f"trades_{suffix}"] = len(swaps_for_pair)
            # Get Volumes
            swaps_volumes = helper.get_swaps_volumes(swaps_for_pair, self.is_reversed)
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
                if data['base'] == "EMC2_KMD":
                    logger.info(last_swap)
                data["last_swap_price"] = last_swap["last_swap_price"]
                data["last_swap_time"] = last_swap["last_swap_time"]
                data["last_swap_uuid"] = last_swap["last_swap_uuid"]
                data["first_swap_price"] = last_swap["first_swap_price"]
                data["first_swap_time"] = last_swap["first_swap_time"]
                data["first_swap_uuid"] = last_swap["first_swap_uuid"]
            msg = f"get_volumes_and_prices for {self.as_str} complete!"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
        except Exception as e:  # pragma: no cover
            msg = f"get_volumes_and_prices for {self.as_str} failed! {e}, returning template"
            return default.result(data=data, msg=msg, loglevel="warning")

    @timed
    def first_last_swap(self, variants: List):
        try:
            data = template.first_last_swap()
            for variant in variants:
                x = template.first_last_swap()

                if variant in self.last_traded_cache:
                    x = self.last_traded_cache[variant]
                elif (
                    transform.invert_pair(variant) in self.last_traded_cache
                ):  # pragma: no cover
                    x = self.last_traded_cache[transform.invert_pair(variant)]

                if x["last_swap_time"] > data["last_swap_time"]:
                    data["last_swap_time"] = x["last_swap_time"]
                    data["last_swap_price"] = x["last_swap_price"]
                    data["last_swap_uuid"] = x["last_swap_uuid"]
                    if self.is_reversed:
                        data["last_swap_price"] = 1 / data["last_swap_price"]

                if data["first_swap_time"] == 0:
                    data["first_swap_time"] = x["first_swap_time"]
                    data["first_swap_price"] = x["first_swap_price"]
                    data["first_swap_uuid"] = x["first_swap_uuid"]
                    if self.is_reversed:
                        data["first_swap_price"] = 1 / data["first_swap_price"]                    

                if x["first_swap_time"] < data["first_swap_time"]:
                    data["first_swap_time"] = x["first_swap_time"]
                    data["first_swap_price"] = x["first_swap_price"]
                    data["first_swap_uuid"] = x["first_swap_uuid"]
                    if self.is_reversed:
                        data["first_swap_price"] = 1 / data["first_swap_price"]

            msg = f"Got first and last swap for {self.as_str}"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
        except Exception as e:  # pragma: no cover
            data = template.first_last_swap()
            msg = f"Returning template for {self.as_str} ({e})"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

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
            msg = f"Got Liquidity for {self.as_str}"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
        except Exception as e:  # pragma: no cover
            data = template.liquidity()
            msg = f"Returning template for {self.as_str} ({e})"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

    @timed
    def ticker_info(self, days=1, all: bool = False):
        # TODO: ps: in order for CoinGecko to show +2/-2% depth,
        # DEX has to provide the formula for +2/-2% depth.
        suffix = transform.get_suffix(days)
        data = template.ticker_info(suffix, self.base, self.quote)
        try:
            data.update(
                {
                    "ticker_id": self.as_str,
                    "base_currency": self.base,
                    "base_usd_price": self.base_usd_price,
                    "quote_currency": self.quote,
                    "quote_usd_price": self.quote_usd_price,
                }
            )

            vol_price_data = self.get_volumes_and_prices(days, all=all)
            data.update(
                {
                    "variants": vol_price_data["variants"],
                    f"trades_{suffix}": f'{vol_price_data[f"trades_{suffix}"]}',
                    "base_volume": vol_price_data["base_volume"],
                    "quote_volume": vol_price_data["quote_volume"],
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
                    f"highest_price_{suffix}": vol_price_data[
                        f"highest_price_{suffix}"
                    ],
                    f"lowest_price_{suffix}": vol_price_data[f"lowest_price_{suffix}"],
                    f"combined_volume_usd": vol_price_data["combined_volume_usd"],
                    "base_volume_usd": vol_price_data["base_volume_usd"],
                    "quote_volume_usd": vol_price_data["quote_volume_usd"],
                    f"price_change_pct_{suffix}": vol_price_data[
                        f"price_change_pct_{suffix}"
                    ],
                    f"price_change_{suffix}": vol_price_data[f"price_change_{suffix}"],
                }
            )

            orderbook_data = self.orderbook.for_pair(depth=100, all=all)
            data.update(
                {
                    "highest_bid": self.orderbook.find_highest_bid(orderbook_data),
                    "lowest_ask": self.orderbook.find_lowest_ask(orderbook_data),
                }
            )

            liquidity = self.get_liquidity(orderbook_data)
            data.update(
                {
                    "liquidity_in_usd": liquidity["liquidity_usd"],
                    "base_liquidity_coins": liquidity["base_liquidity_coins"],
                    "base_liquidity_usd": liquidity["base_liquidity_usd"],
                    "quote_liquidity_coins": liquidity["rel_liquidity_coins"],
                    "quote_liquidity_usd": liquidity["rel_liquidity_usd"],
                }
            )
            msg = f"Completed ticker info for {self.as_str}"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
        except Exception as e:  # pragma: no cover
            msg = f"ticker_info for {self.as_str} failed! {e}"
            return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

    @timed
    def get_swap_prices(self, swaps_for_pair, is_reversed):
        data = {}
        try:
            [data.update(helper.get_price_at_finish(i, is_reversed)) for i in swaps_for_pair]
        except Exception as e:  # pragma: no cover
            msg = f"get_swap_prices for {self.as_str} failed!"
            return default.result(data=data, msg=msg, loglevel="warning")
        msg = f"Completed get_swap_prices info for {self.as_str}"
        return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

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
        except Exception as e:  # pragma: no cover
            data = []
            msg = f"{self.as_str} pair_swaps failed!"
            return default.result(data=data, msg=msg, loglevel="warning")
        msg = f"Completed pair_swaps for {self.as_str}"
        return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)

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
                data = {"uuids": data["ALL"], "variants": variants}
            elif self.as_str in data:
                data = {"uuids": data[self.as_str], "variants": [self.as_str]}
            elif transform.invert_pair(self.as_str) in data:
                data = {
                    "uuids": data[transform.invert_pair(self.as_str)],
                    "variants": [transform.invert_pair(self.as_str)],
                }

        except Exception as e:  # pragma: no cover
            data = {"uuids": [], "variants": [self.as_str]}
            msg = f"{self.as_str} swap_uuids failed!"
            return default.result(data=data, msg=msg, loglevel="warning")
        msg = f"Completed swap_uuids for {self.as_str}"
        return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)


@timed
def get_all_coin_pairs(coin, priced_coins):
    try:
        pairs = [
            (f"{i}_{coin}") for i in priced_coins if coin not in [i, f"{i}-segwit"]
        ]
        data = list(set([transform.order_pair_by_market_cap(i) for i in pairs]))

    except Exception as e:  # pragma: no cover
        data = []
        msg = f"{coin} get_all_coin_pairs failed!"
        return default.result(data=data, msg=msg, loglevel="warning")
    msg = f"Completed get_all_coin_pairs for {coin}"
    return default.result(data=data, msg=msg, loglevel="pair", ignore_until=2)
