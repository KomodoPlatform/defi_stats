#!/usr/bin/env python3
import inspect
from os.path import basename
import time
import sqlite3
from typing import List, Optional, Any

from decimal import Decimal
from datetime import datetime, timedelta
from random import randrange
from const import templates
from lib.cache_load import load_gecko_source
from lib.cache_item import CacheItem
from util.helper import (
    order_pair_by_market_cap,
    sort_dict,
    get_all_coin_pairs,
    valid_coins,
    get_netid,
)
from util.files import Files
from util.utils import Utils
from util.enums import TradeType
from util.exceptions import RequiredQueryParamMissing
from util.logger import logger, get_trace, StopWatch

get_stopwatch = StopWatch


class SqliteQuery:
    def __init__(self, db, **kwargs):
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            self.kwargs = kwargs
            self.options = ["testing", "dict_format"]
            templates.set_params(self, self.kwargs, self.options)
            self.db = db
            self.db_path = db.db_path
            self.db_file = basename(db.db_path)
            self.netid = get_netid(db.db_file)
            self.conn = db.conn
            if self.dict_format:
                self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            self.utils = Utils(testing=self.testing)
            self.files = Files(testing=self.testing)
            self.gecko_source = load_gecko_source()
        except Exception as e:
            error = f"{type(e)}: Failed to init SqliteQuery: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return

    @property
    def tables(self):
        self.sql_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [i[0] for i in self.sql_cursor.fetchall()]

    def get_table_columns(self, table):
        sql = f"SELECT * FROM '{table}' LIMIT 1;"
        r = self.sql_cursor.execute(sql)
        r.fetchone()
        return [i[0] for i in r.description]

    def get_pairs(self, days: int = 7, include_all_kmd=True) -> list:
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        """
        Returns an alphabetically sorted list of pairs
        (as a list of tuples) with at least one successful
        swap in the last 'x' days. ('BASE', 'REL') tuples
        are sorted by market cap to conform to CEX standards.
        """
        try:
            timestamp = int(time.time() - 86400 * days)
            sql = f"SELECT DISTINCT maker_coin_ticker, maker_coin_platform, \
                    taker_coin_ticker, taker_coin_platform FROM stats_swaps \
                    WHERE finished_at > {timestamp} AND is_success=1;"
            self.sql_cursor.execute(sql)
            data = self.sql_cursor.fetchall()
            # Cover the variants
            pairs = [
                (f"{i[0]}-{i[1]}", f"{i[2]}-{i[3]}")
                for i in data
                if i[1] not in ["", "segwit"] and i[3] not in ["", "segwit"]
            ]
            pairs += [
                (f"{i[0]}-{i[1]}", f"{i[2]}")
                for i in data
                if i[1] not in ["", "segwit"] and i[3] in ["", "segwit"]
            ]
            pairs += [
                (f"{i[0]}", f"{i[2]}-{i[3]}")
                for i in data
                if i[1] in ["", "segwit"] and i[3] not in ["", "segwit"]
            ]
            pairs += [
                (f"{i[0]}", f"{i[2]}")
                for i in data
                if i[1] in ["", "segwit"] and i[3] in ["", "segwit"]
            ]
            if include_all_kmd:
                coins = valid_coins(CacheItem("coins_config").data)
                pairs += get_all_coin_pairs("KMD", coins)
            # Sort pair by ticker to expose base-rel & rel-base duplicates
            sorted_pairs = [tuple(sorted(pair)) for pair in pairs]
            # Remove the duplicates
            pairs = list(set(sorted_pairs))
            # Sort the pair tickers with lower MC first & higher MC second
            data = sorted(
                [order_pair_by_market_cap(pair, self.gecko_source) for pair in pairs]
            )
        except Exception as e:
            error = f"{type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=f"{context}")
            return

        context = f"[{len(pairs)} pairs] [{days} days] [netid {self.netid}] ok"
        get_stopwatch(start, query=True, context=context)
        return data

    def get_swaps_for_pair(
        self,
        pair: tuple,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 0,
        start_time: int = 0,
        end_time: int = 0,
        reverse=False,
    ) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        # logger.warning(pair)
        n = 0
        while True:
            try:
                if end_time == 0:
                    end_time = int(time.time())
                pair = order_pair_by_market_cap(pair, self.gecko_source)
                # We stripped segwit from the pairs in get_pairs()
                # so we need to add it back here if it's present
                segwit_coins = self.utils.segwit_coins()
                if reverse:
                    base = [pair[1]]
                    quote = [pair[0]]
                    if pair[0] in segwit_coins:
                        quote.append(f"{pair[0]}-segwit")
                    if pair[1] in segwit_coins:
                        base.append(f"{pair[1]}-segwit")
                else:
                    base = [pair[0]]
                    quote = [pair[1]]
                    if pair[0] in segwit_coins:
                        base.append(f"{pair[0]}-segwit")
                    if pair[1] in segwit_coins:
                        quote.append(f"{pair[1]}-segwit")

                swaps_for_pair = []
                self.conn.row_factory = sqlite3.Row
                self.sql_cursor = self.conn.cursor()
                for i in base:
                    for j in quote:
                        base_ticker = i.split("-")[0]
                        quote_ticker = j.split("-")[0]
                        base_platform = ""
                        quote_platform = ""
                        if len(i.split("-")) == 2:
                            base_platform = i.split("-")[1]
                        if len(j.split("-")) == 2:
                            quote_platform = j.split("-")[1]

                        sql = "SELECT * FROM stats_swaps WHERE"
                        sql += f" finished_at > {start_time}"
                        sql += f" AND finished_at < {end_time}"
                        sql += f" AND maker_coin_ticker='{base_ticker}'"
                        sql += f" AND taker_coin_ticker='{quote_ticker}'"
                        sql += f" AND maker_coin_platform='{base_platform}'"
                        sql += f" AND taker_coin_platform='{quote_platform}'"
                        sql += " AND is_success=1 ORDER BY finished_at DESC"
                        if limit > 0:
                            sql += f" LIMIT {limit}"
                        sql += ";"

                        self.sql_cursor.execute(sql)
                        data = self.sql_cursor.fetchall()
                        swaps_for_pair_a_b = [dict(row) for row in data]

                        for swap in swaps_for_pair_a_b:
                            swap["trade_type"] = "buy"

                        sql = "SELECT * FROM stats_swaps WHERE"
                        sql += f" finished_at > {start_time}"
                        sql += f" AND finished_at < {end_time}"
                        sql += f" AND taker_coin_ticker='{base_ticker}'"
                        sql += f" AND maker_coin_ticker='{quote_ticker}'"
                        sql += f" AND taker_coin_platform='{base_platform}'"
                        sql += f" AND maker_coin_platform='{quote_platform}'"
                        sql += " AND is_success=1 ORDER BY finished_at DESC"
                        if limit > 0:
                            sql += f" LIMIT {limit}"
                        sql += ";"
                        # logger.warning(sql)
                        self.sql_cursor.execute(sql)
                        data = self.sql_cursor.fetchall()
                        swaps_for_pair_b_a = [dict(row) for row in data]

                        for swap in swaps_for_pair_b_a:
                            # A little slieght of hand for reverse pairs
                            temp_maker_amount = swap["maker_amount"]
                            swap["maker_amount"] = swap["taker_amount"]
                            swap["taker_amount"] = temp_maker_amount
                            swap["trade_type"] = "sell"

                        swaps_for_pair += swaps_for_pair_a_b + swaps_for_pair_b_a
                # Sort swaps by timestamp
                swaps_for_pair = sorted(
                    swaps_for_pair, key=lambda k: k["finished_at"], reverse=True
                )
                if trade_type == TradeType.BUY:
                    swaps_for_pair = [
                        swap for swap in swaps_for_pair if swap["trade_type"] == "buy"
                    ]
                elif trade_type == TradeType.SELL:
                    swaps_for_pair = [
                        swap for swap in swaps_for_pair if swap["trade_type"] == "sell"
                    ]
                if limit > 0:
                    swaps_for_pair = swaps_for_pair[:limit]

                if len(swaps_for_pair) > 0:
                    context = f"{len(swaps_for_pair)} {pair} swaps in {self.db_file}"
                    get_stopwatch(start, query=True, context=context)
                else:
                    context = f"0 {pair} swaps in {self.db_file}"
                    # get_stopwatch(start, muted=True, context=context)
                return swaps_for_pair
            except sqlite3.OperationalError as e:
                if n > 10:
                    
                    error = f"{type(e)}: Failed to get_swaps_for_pair with {self.db_file}: {e}"
                    context = get_trace(stack, error)
                    get_stopwatch(start, error=True, context=context)
                    return []
                n += 1
                logger.warning(
                    f"Error in [get_swaps_for_pair] with {self.db_file}: {e}, retrying..."
                )
                time.sleep(randrange(20))
            except Exception as e:  # pragma: no cover
                
                error = (
                    f"{type(e)}: Failed to get_swaps_for_pair with {self.db_file}: {e}"
                )
                context = get_trace(stack, error)
                get_stopwatch(start, error=True, context=context)
                return []

    def get_swap(self, uuid):
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            sql = "SELECT * FROM stats_swaps WHERE"
            sql += f" uuid='{uuid}';"
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            self.sql_cursor.execute(sql)
            data = self.sql_cursor.fetchall()
            data = [dict(row) for row in data]
            if len(data) == 0:
                return {"error": f"swap uuid {uuid} not found"}
            else:
                data = data[0]
            for i in ["taker_coin_usd_price", "maker_coin_usd_price"]:
                if data[i] is None:
                    data[i] = "0"
            context = f"returned data for uuid {uuid}"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            
            error = f" failed: {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return

    def get_row_count(self, table):
        self.sql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        r = self.sql_cursor.fetchone()
        return r[0]


    def get_uuids(self, success_only=True, fail_only=False) -> List:
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            if fail_only:
                self.sql_cursor.execute(
                    "SELECT uuid FROM stats_swaps WHERE is_success = 0"
                )
            elif success_only:
                self.sql_cursor.execute(
                    "SELECT uuid FROM stats_swaps WHERE is_success = 1"
                )
            else:
                self.sql_cursor.execute("SELECT uuid FROM stats_swaps")
            r = self.sql_cursor.fetchall()
            data = [i[0] for i in r]
            context = f"{len(data)} uuids for netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            error = f" failed: {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return []

    def get_pairs_last_trade(self, started_at=None, finished_at=None, min_swaps=5):
        # TODO: Filter out test coins
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            sql = "SELECT taker_coin_ticker, maker_coin_ticker, \
                    taker_coin_platform, maker_coin_platform, \
                    taker_amount AS last_taker_amount, \
                    maker_amount AS last_maker_amount, \
                    MAX(finished_at) AS last_swap_time, \
                    COUNT(uuid) AS swap_count, \
                    SUM(taker_amount) AS sum_taker_traded, \
                    SUM(maker_amount) AS sum_maker_traded \
                    FROM stats_swaps"

            sql += " WHERE is_success=1"
            if started_at is not None:
                sql += f" AND finished_at > {started_at}"
            if finished_at is not None:
                sql += f" AND finished_at < {finished_at}"
            sql += " GROUP BY taker_coin_ticker, maker_coin_ticker, \
                    taker_coin_platform, maker_coin_platform;"
            self.sql_cursor = self.conn.cursor()
            self.sql_cursor.execute(sql)
            resp = self.sql_cursor.fetchall()
            resp = [dict(i) for i in resp if i["swap_count"] >= min_swaps]
            by_pair_dict = {}
            for i in resp:
                item = {}
                for k, v in i.items():
                    if k not in [
                        "taker_coin_ticker",
                        "maker_coin_ticker",
                        "taker_coin_platform",
                        "maker_coin_platform",
                    ]:
                        item.update({k: v})
                if i["taker_coin_platform"] in ["", "segwit"]:
                    taker = i["taker_coin_ticker"]
                else:
                    taker = f'{i["taker_coin_ticker"]}-{i["taker_coin_platform"]}'
                if i["maker_coin_platform"] in ["", "segwit"]:
                    maker = i["maker_coin_ticker"]
                else:
                    maker = f'{i["maker_coin_ticker"]}-{i["maker_coin_platform"]}'

                ticker = order_pair_by_market_cap((taker, maker), self.gecko_source)
                ticker = "_".join(ticker)
                # Handle segwit
                if ticker not in by_pair_dict:
                    by_pair_dict.update({ticker: item})
                elif item["last_swap_time"] > by_pair_dict[ticker]["last_swap_time"]:
                    by_pair_dict.update({ticker: item})
            sorted_dict = sort_dict(by_pair_dict)
            context = f"{len(sorted_dict)} pairs returned for netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return sorted_dict
        except Exception as e:
            
            error = f"{type(e)}: Failed: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return []

    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            swap_price = None
            swap_time = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{base.split('-')[0]}' \
                    AND taker_coin_ticker='{quote.split('-')[0]}' AND is_success=1"
            if len(base.split("-")) == 2:
                if base.split("-")[1] != "segwit":
                    platform = base.split("-")[1]
                    sql += f" AND maker_coin_platform='{platform}'"
            if len(quote.split("-")) == 2:
                if quote.split("-")[1] != "segwit":
                    platform = quote.split("-")[1]
                    sql += f" AND taker_coin_platform='{platform}'"
            sql += " ORDER BY finished_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp = self.sql_cursor.fetchone()
            if resp is not None:
                swap_price = Decimal(resp["taker_amount"]) / Decimal(
                    resp["maker_amount"]
                )
                swap_time = resp["finished_at"]

            swap_price2 = None
            swap_time2 = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{quote.split('-')[0]}' \
                    AND taker_coin_ticker='{base.split('-')[0]}' AND is_success=1"
            if len(base.split("-")) == 2:
                if base.split("-")[1] != "segwit":
                    platform = base.split("-")[1]
                    sql += f" AND taker_coin_platform='{platform}'"
            if len(quote.split("-")) == 2:
                if quote.split("-")[1] != "segwit":
                    platform = quote.split("-")[1]
                    sql += f" AND maker_coin_platform='{platform}'"
            sql += " ORDER BY finished_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp2 = self.sql_cursor.fetchone()
            if resp2 is not None:
                swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                    resp2["taker_amount"]
                )
                swap_time2 = resp2["finished_at"]
            if swap_price and swap_price2:
                if swap_time > swap_time2:
                    price = swap_price
                    last_swap_time = swap_time
                else:
                    price = swap_price2
                    last_swap_time = swap_time2
            elif swap_price:
                price = swap_price
                last_swap_time = swap_time
            elif swap_price2:
                price = swap_price2
                last_swap_time = swap_time2
            else:
                price = 0
                last_swap_time = 0
            data = {
                "price": price,
                "timestamp": last_swap_time,
            }
            context = f"{base}/{quote}: {data} (netid {self.netid})"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            
            error = f"failed, returning template  {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return templates.last_price_for_pair()

    # This was a duplicate of SqliteQuery.get_atomicdexio
    def swap_counts(self):
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
            timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))

            self.sql_cursor
            self.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE is_success=1;"
            )
            swaps_all_time = self.sql_cursor.fetchone()[0]
            self.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE started_at > ? AND is_success=1;",
                (timestamp_24h_ago,),
            )
            swaps_24h = self.sql_cursor.fetchone()[0]
            self.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE started_at > ? AND is_success=1;",
                (timestamp_30d_ago,),
            )
            swaps_30d = self.sql_cursor.fetchone()[0]
            self.conn.close()
            data = {
                "swaps_all_time": swaps_all_time,
                "swaps_30d": swaps_30d,
                "swaps_24h": swaps_24h,
            }
            context = f"{data} for netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            
            error = (
                f" failed, returning template {type(e)}: {e}"
            )
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return templates.swap_counts()

    def get_swaps_for_ticker(
        self,
        ticker: str,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 0,
        start_time: int = int(time.time()) - 86400,
        end_time: int = 0,
    ) -> list:
        """
        Returns a list of swaps for a given ticker between two timestamps.
        If no timestamp is given, returns all swaps for the ticker.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        # logger.warning(pair)
        try:
            tickers = []
            if end_time == 0:
                end_time = int(time.time())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = self.utils.segwit_coins()
            if ticker in segwit_coins:
                tickers.append(f"{ticker}-segwit")

            swaps_for_ticker = []
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            for i in tickers:
                base_ticker = i.split("-")[0]
                base_platform = ""
                if len(i.split("-")) == 2:
                    base_platform = i.split("-")[1]

                sql = "SELECT * FROM stats_swaps WHERE"
                sql += f" finished_at > {start_time}"
                sql += f" AND finished_at < {end_time}"
                sql += f" AND maker_coin_ticker='{base_ticker}'"
                sql += f" AND maker_coin_platform='{base_platform}'"
                sql += " AND is_success=1 ORDER BY finished_at DESC"
                if limit > 0:
                    sql += f" LIMIT {limit}"
                sql += ";"

                self.sql_cursor.execute(sql)
                data = self.sql_cursor.fetchall()
                swaps_as_maker = [dict(row) for row in data]

                for swap in swaps_as_maker:
                    swap["trade_type"] = "sell"

                sql = "SELECT * FROM stats_swaps WHERE"
                sql += f" finished_at > {start_time}"
                sql += f" AND finished_at < {end_time}"
                sql += f" AND taker_coin_ticker='{base_ticker}'"
                sql += f" AND taker_coin_platform='{base_platform}'"
                sql += " AND is_success=1 ORDER BY finished_at DESC"
                if limit > 0:
                    sql += f" LIMIT {limit}"
                sql += ";"
                # logger.warning(sql)
                self.sql_cursor.execute(sql)
                data = self.sql_cursor.fetchall()
                swaps_as_taker = [dict(row) for row in data]

                for swap in swaps_as_taker:
                    swap["trade_type"] = "buy"

                swaps_for_ticker += swaps_as_maker + swaps_as_taker
            # Sort swaps by timestamp
            data = sorted(
                swaps_for_ticker, key=lambda k: k["finished_at"], reverse=True
            )
            if trade_type == TradeType.BUY:
                data = [swap for swap in data if swap["trade_type"] == "buy"]
            elif trade_type == TradeType.SELL:
                data = [swap for swap in data if swap["trade_type"] == "sell"]
            if limit > 0:
                data = data[:limit]
            context = f"swaps info for {len(data)} pairs in netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            
            error = f"failed, returning template: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return []

    def get_volume_for_ticker(
        self,
        ticker: str,
        trade_type: str,
        start_time: int = 0,
        end_time: int = 0,
    ) -> list:
        """
        Returns volume traded of ticker between two timestamps.
        If no timestamp is given, returns all swaps for the ticker.
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            logger.info(
                f"Getting volume for {ticker} between {start_time} and {end_time}"
            )
            tickers = [ticker]
            if end_time == 0:
                end_time = int(time.time())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = self.utils.segwit_coins()
            if ticker in segwit_coins:
                tickers.append(f"{ticker}-segwit")

            volume_for_ticker = 0
            self.sql_cursor = self.conn.cursor()
            for i in tickers:
                base_ticker = i.split("-")[0]
                base_platform = ""
                if len(i.split("-")) == 2:
                    base_platform = i.split("-")[1]

                volume_as_maker = 0
                if trade_type in [TradeType.BUY, TradeType.ALL]:
                    sql = "SELECT SUM(CAST(maker_amount AS NUMERIC)) FROM stats_swaps WHERE"
                    sql += f" finished_at > {start_time}"
                    sql += f" AND finished_at < {end_time}"
                    sql += f" AND maker_coin_ticker='{base_ticker}'"
                    sql += f" AND maker_coin_platform='{base_platform}'"
                    sql += " AND is_success=1 ORDER BY finished_at DESC;"
                    self.sql_cursor.execute(sql)
                    data = self.sql_cursor.fetchone()
                    if data[0] is not None:
                        volume_as_maker = data[0]

                volume_as_taker = 0
                if trade_type in [TradeType.SELL, TradeType.ALL]:
                    sql = "SELECT SUM(CAST(taker_amount as NUMERIC)) FROM stats_swaps WHERE"
                    sql += f" finished_at > {start_time}"
                    sql += f" AND finished_at < {end_time}"
                    sql += f" AND taker_coin_ticker='{base_ticker}'"
                    sql += f" AND taker_coin_platform='{base_platform}'"
                    sql += " AND is_success=1 ORDER BY finished_at DESC;"
                    self.sql_cursor.execute(sql)
                    data = self.sql_cursor.fetchone()
                    if data[0] is not None:
                        volume_as_taker = data[0]

                volume_for_ticker += volume_as_maker + volume_as_taker

            context = f"{volume_for_ticker} for {ticker} on netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return volume_for_ticker
        except Exception as e:
            
            error = f"failed, returning template: {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return 0

    # Post NetId Migration below

    def build_query(self, **kwargs) -> str:
        """
        Avaliable filters:
            - start: epoch timestamp (integer)
            - end: epoch timestamp (integer)
            - cols: columns to return values for (list, default '*')
            - count: count of records returned (boolean, default false)
            - sum: sum of column matching filter (boolean, default false)
            - sum_field: column to return the sum of (string)
            - limit: number of records to return. (integer, default 1000)
            - success_only: only successful swaps (boolean, default true)
            - failed_only: only failed swaps (boolean, default false)
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        sql = ""
        try:
            if "table" in kwargs:
                sql = f"SELECT * FROM {kwargs['table']}"
            else:
                raise RequiredQueryParamMissing(
                    "You need to specify a value for 'table'"
                )
            if "sum" in kwargs and "sum_field" in kwargs == "":
                msg = "If calculating sum, you need to specify the sum_field"
                context = f"failed, sql = {msg}"
                get_stopwatch(start, error=True, context=context)

                return []
            if "success_only" in kwargs and "failed_only" in kwargs:
                msg = "Cant set `success_only` and `failed_only` to true at same time"
                context = f"failed, sql = {msg}"
                get_stopwatch(start, error=True, context=context)
                return []

            if "cols" in kwargs:
                cols = kwargs["cols"]
                sql.replace("*", f"({', '.join(cols)})")

            if "sum" in kwargs and "count" in kwargs:
                sql.replace(
                    "*", f"SUM({kwargs['sum_field']}), COUNT({kwargs['sum_field']})"
                )
            elif "count" in kwargs:
                sql.replace("*", "COUNT(*)")
            elif "sum" in kwargs:
                sql.replace("*", f"SUM({kwargs['sum_field']})")

            sql += (
                f" WHERE started_at > {kwargs['start']} AND finished_at {kwargs['end']}"
            )
            if "success_only" in kwargs:
                sql += " AND is_success=1"
            elif "failed_only" in kwargs:
                sql += " AND is_success=0"
            if "filter_sql" in kwargs:
                sql += kwargs["filter_sql"].replace("WHERE", "AND")
            context = f"complete for netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return sql
        except Exception as e:
            error = f"failed, sql = {sql}: {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return sql

    def get_timespan_swaps(self, **kwargs) -> list:
        """
        Returns a list of swaps between two timestamps
        """
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        try:
            sql = self.build_query(**kwargs)
            self.sql_cursor.execute(sql)
            data = self.sql_cursor.fetchall()
            context = f"{len(data)} swaps for netid {self.netid}"
            get_stopwatch(start, query=True, context=context)
            return data
        except Exception as e:
            
            error = f"failed: {type(e)}: {e}"
            context = get_trace(stack, error)
            get_stopwatch(start, error=True, context=context)
            return []
