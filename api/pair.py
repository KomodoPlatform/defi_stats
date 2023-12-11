#!/usr/bin/env python3
import time
import sqlite3
from collections import OrderedDict
from decimal import Decimal
from logger import logger
from const import MM2_DB_PATH_7777
from helper import (
    sum_json_key,
    sum_json_key_10f,
    format_10f,
    list_json_key,
    sort_dict_list,
    order_pair_by_market_cap,
    set_pair_as_tuple,
)
from const import MM2_HOST
from orderbook import Orderbook
from generics import Files, Templates
from utils import Utils
from db import get_sqlite_db
from enums import TradeType


class Pair:
    """
    Allows for referencing pairs as a string or tuple.
    To standardise like CEX pairs, the higher Mcap coin is always second.
    e.g. DOGE_BTC, not BTC_DOGE
    """

    def __init__(
        self,
        pair,
        testing: bool = False,
        path_to_db=MM2_DB_PATH_7777,
        mm2_host=MM2_HOST,
        mm2_port=7877,
    ):
        self.path_to_db = path_to_db
        self.mm2_host = mm2_host
        self.mm2_port = mm2_port
        self.mm2_rpc = f"{mm2_host}:{mm2_port}"
        self.testing = testing
        self.files = Files(testing=self.testing)
        self.utils = Utils(testing=self.testing)
        self.templates = Templates()
        self.gecko_source = self.utils.load_jsonfile(self.files.gecko_source_file)
        self.as_tuple = order_pair_by_market_cap(
            set_pair_as_tuple(pair), self.gecko_source
        )
        self.as_str = self.as_tuple[0] + "_" + self.as_tuple[1]
        self.base = self.as_tuple[0]
        self.quote = self.as_tuple[1]
        self.base_coin = self.as_tuple[0].split("-")[0]
        self.quote_coin = self.as_tuple[1].split("-")[0]
        if len(self.as_tuple[0].split("-")) == 2:
            self.base_platform = self.as_tuple[0].split("-")[1]
        if len(self.as_tuple[1].split("-")) == 2:
            self.quote_platform = self.as_tuple[1].split("-")[1]

        self.base_price = self.utils.get_gecko_usd_price(
            self.base_coin, self.gecko_source
        )
        self.quote_price = self.utils.get_gecko_usd_price(
            self.quote_coin, self.gecko_source
        )
        self.orderbook = Orderbook(
            pair=self, testing=self.testing, mm2_host=MM2_HOST, mm2_port=7877
        )
        self.orderbook_data = self.orderbook.for_pair(endpoint=False)
        self.info = {
            "ticker_id": self.as_str,
            "pool_id": self.as_str,
            "base": self.base,
            "target": self.quote,
        }

    def historical_trades(
        self,
        trade_type: TradeType,
        netid: int,
        limit: int = 100,
        start_time: int = 0,
        end_time: int = 0,
        DB=None,
        reverse=False,
    ):
        """Returns trades for this pair."""
        try:
            if start_time == 0:
                start_time = int(time.time()) - 86400
            if end_time == 0:
                end_time = int(time.time())
            trades_info = []
            DB = get_sqlite_db(
                path_to_db=self.path_to_db, testing=self.testing, DB=DB, netid=netid
            )

            swaps_for_pair = DB.get_swaps_for_pair(
                self.as_tuple,
                limit=limit,
                trade_type=trade_type,
                start_time=start_time,
                end_time=end_time,
                reverse=reverse,
            )
            logger.debug(f"{len(swaps_for_pair)} swaps_for_pair: {self.as_str}")
            for swap in swaps_for_pair:
                trade_info = OrderedDict()
                price = Decimal(swap["taker_amount"]) / Decimal(swap["maker_amount"])
                trade_info["trade_id"] = swap["uuid"]
                trade_info["base_ticker"] = self.base
                trade_info["target_ticker"] = self.quote
                trade_info["price"] = format_10f(price)
                trade_info["base_volume"] = format_10f(swap["maker_amount"])
                trade_info["quote_volume"] = format_10f(swap["taker_amount"])
                trade_info["target_volume"] = format_10f(swap["taker_amount"])
                trade_info["timestamp"] = swap["finished_at"]
                trade_info["type"] = swap["trade_type"]
                trades_info.append(trade_info)
        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [Pair.trades]: {e}")
            trades_info = []

        average_price = self.get_average_price(trades_info)
        buys = list_json_key(trades_info, "type", "buy")
        sells = list_json_key(trades_info, "type", "sell")
        buys = sort_dict_list(buys, "timestamp", reverse=True)
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

        return data

    def get_average_price(self, trades_info):
        if len(trades_info) > 0:
            return sum_json_key(trades_info, "price") / len(trades_info)
        return 0

    def get_volumes_and_prices(self, days: int = 1, DB=None):
        """
        Iterates over list of swaps to get volumes and prices data
        """
        DB = get_sqlite_db(path_to_db=self.path_to_db, testing=self.testing, DB=DB)
        suffix = self.utils.get_suffix(days)
        data = self.templates.volumes_and_prices(suffix)
        timestamp = int(time.time() - 86400 * days)
        try:
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple, start_time=timestamp)
        except Exception as e:
            logger.warning(f"{type(e)} Error in [Pair.get_volumes_and_prices]: {e}")
            return data
        data["base"] = self.base
        data["quote"] = self.quote
        data["base_price"] = self.base_price
        data["quote_price"] = self.quote_price
        num_swaps = len(swaps_for_pair)
        data["trades_24hr"] = num_swaps
        swap_prices = self.get_swap_prices(swaps_for_pair)
        swaps_volumes = self.get_swaps_volumes(swaps_for_pair)
        data["base_volume"] = swaps_volumes[0]
        data["quote_volume"] = swaps_volumes[1]
        data["base_volume_usd"] = Decimal(swaps_volumes[0]) * Decimal(self.base_price)
        data["quote_volume_usd"] = Decimal(swaps_volumes[1]) * Decimal(self.quote_price)
        data["combined_volume_usd"] = (
            data["base_volume_usd"] + data["quote_volume_usd"]
        ) / 2

        if len(swap_prices) > 0:
            # TODO: using timestamps as an index works for now,
            # but breaks when two swaps have the same timestamp.
            last_swap = DB.get_last_price_for_pair(self.base, self.quote)
            highest_price = max(swap_prices.values())
            lowest_price = min(swap_prices.values())
            newest_price = swap_prices[max(swap_prices.keys())]
            oldest_price = swap_prices[min(swap_prices.keys())]
            price_change = Decimal(newest_price) - Decimal(oldest_price)
            pct_change = Decimal(newest_price) / Decimal(oldest_price) - 1

            data[f"highest_price_{suffix}"] = highest_price
            data[f"lowest_price_{suffix}"] = lowest_price
            data["last_price"] = last_swap["price"]
            data["last_trade"] = last_swap["timestamp"]
            data[f"price_change_percent_{suffix}"] = pct_change
            data[f"price_change_{suffix}"] = price_change
        return data

    def get_liquidity(self):
        """Liquidity for pair from current orderbook & usd price."""
        x = isinstance(self.orderbook_data, dict)
        if not x:
            logger.warning(f"{self.as_str} {x}")
            logger.warning(type(self.orderbook_data))
        if "total_asks_base_vol" in self.orderbook_data:
            base_liq_coins = Decimal(self.orderbook_data["total_asks_base_vol"])
            rel_liq_coins = Decimal(self.orderbook_data["total_bids_quote_vol"])
        else:
            base_liq_coins = Decimal(0)
            rel_liq_coins = Decimal(0)
        base_liq_usd = Decimal(self.base_price) * Decimal(base_liq_coins)
        rel_liq_coins = Decimal(rel_liq_coins)
        rel_liq_usd = Decimal(self.quote_price) * Decimal(rel_liq_coins)
        rel_liq_usd = Decimal(rel_liq_usd)
        base_liq_usd = Decimal(base_liq_usd)
        return {
            "rel_usd_price": self.quote_price,
            "rel_liquidity_coins": rel_liq_coins,
            "rel_liquidity_usd": rel_liq_usd,
            "base_usd_price": self.base_price,
            "base_liquidity_coins": base_liq_coins,
            "base_liquidity_usd": base_liq_usd,
            "liquidity_usd": base_liq_usd + rel_liq_usd,
        }

    def gecko_ticker_info(self, days=1, DB=None, exclude_unpriced=True):
        # TODO: ps: in order for CoinGecko to show +2/-2% depth,
        # DEX has to provide the formula for +2/-2% depth.
        try:
            # To exclude unpriced pairs
            if (self.base_price == 0 or self.quote_price == 0) and exclude_unpriced:
                err = {"error": f"Excluding {self.as_str} because it's unpriced"}
                # logger.debug(err)
                return err
            DB = get_sqlite_db(path_to_db=self.path_to_db, testing=self.testing, DB=DB)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            data = self.get_volumes_and_prices(days, DB=DB)
            suffix = self.utils.get_suffix(days)
            liquidity = self.get_liquidity()

            resp = {
                "ticker_id": self.as_str,
                "pool_id": self.as_str,
                "base_currency": self.base,
                "target_currency": self.quote,
                "last_price": format_10f(data["last_price"]),
                "last_trade": f'{data["last_trade"]}',
                "trades_24hr": f'{data["trades_24hr"]}',
                "base_volume": data["base_volume"],
                "target_volume": data["quote_volume"],
                "base_usd_price": self.base_price,
                "target_usd_price": self.quote_price,
                "bid": self.utils.find_highest_bid(self.orderbook_data),
                "ask": self.utils.find_lowest_ask(self.orderbook_data),
                "high": format_10f(data[f"highest_price_{suffix}"]),
                "low": format_10f(data[f"lowest_price_{suffix}"]),
                "volume_usd_24hr": format_10f(data["combined_volume_usd"]),
                "liquidity_in_usd": format_10f(liquidity["liquidity_usd"]),
                f"price_change_percent_{suffix}": data[
                    f"price_change_percent_{suffix}"
                ],
                f"price_change_{suffix}": data[f"price_change_{suffix}"],
            }
            return resp

        except Exception as e:  # pragma: no cover
            err = {"error": f"Error in [Pair.ticker] {self.as_str}: {e}"}
            logger.warning(err)
            return err

    def get_swap_prices(self, swaps_for_pair):
        data = {}
        [
            data.update(
                {
                    i["finished_at"]: Decimal(i["taker_amount"])
                    / Decimal(i["maker_amount"])
                }
            )
            for i in swaps_for_pair
        ]
        return data

    def swap_uuids(self, start_time: int, end_time: int = 0, DB=None) -> list:
        DB = get_sqlite_db(path_to_db=self.path_to_db, testing=self.testing, DB=DB)
        swaps_for_pair = DB.get_swaps_for_pair(
            self.as_tuple, start_time=start_time, end_time=end_time
        )
        data = [i["uuid"] for i in swaps_for_pair]
        return data

    def get_swaps_volumes(self, swaps_for_pair):
        try:
            return [
                sum_json_key_10f(swaps_for_pair, "maker_amount"),
                sum_json_key_10f(swaps_for_pair, "taker_amount"),
            ]
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)} Error in [get_swaps_volumes]: {e}")
            return [0, 0]
