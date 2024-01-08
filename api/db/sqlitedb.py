#!/usr/bin/env python3
import os
from os.path import basename
import time
import sqlite3
from typing import List
from decimal import Decimal
from datetime import datetime, timedelta
from const import MM2_DB_PATHS, MM2_NETID, compare_fields
from util.defaults import default_result, set_params, default_error
from util.enums import TradeType, TablesEnum, NetId, ColumnsEnum
from util.exceptions import InvalidParamCombination
from util.files import Files
from util.logger import logger, timed
from util.transform import sort_dict, order_pair_by_market_cap, format_10f
import lib


class SqliteDB:  # pragma: no cover
    def __init__(self, db_path, **kwargs):
        try:
            self.kwargs = kwargs
            self.start = int(time.time())
            self.db_path = db_path
            self.db_file = basename(self.db_path)
            self.netid = get_netid(self.db_file)
            self.options = ["testing", "wal", "netid"]
            set_params(self, self.kwargs, self.options)

            if "last_traded_cache" in kwargs:
                self.last_traded_cache = kwargs["last_traded_cache"]
            else:
                self.last_traded_cache = lib.load_generic_last_traded(testing=self.testing)

            if "coins_config" in kwargs:
                self.coins_config = kwargs["coins_config"]
            else:
                # logger.loop(f"Getting coins_config for db")
                self.coins_config = lib.load_coins_config(testing=self.testing)

            if "gecko_source" in kwargs:
                self.gecko_source = kwargs["gecko_source"]
            else:
                # logger.loop(f"Getting gecko_source for db")
                self.gecko_source = lib.load_gecko_source(testing=self.testing)

            self.conn = self.connect()
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            if self.wal:
                sql = "PRAGMA journal_mode=WAL;"
                self.sql_cursor.execute(sql)
                self.sql_cursor.fetchall()
            self.query = SqliteQuery(db=self, **self.kwargs)
            self.update = SqliteUpdate(db=self, **self.kwargs)
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)}: Failed to init SqliteDB: {e}")

    @timed
    def close(self):
        self.conn.close()
        msg = f"Connection to {self.db_file} closed"
        return default_result(msg=msg, loglevel="debug", ignore_until=10)

    def connect(self):
        return sqlite3.connect(self.db_path)


class SqliteQuery:  # pragma: no cover
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
            self.db = db

        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)}: Failed to init SqliteQuery: {e}")

    @property
    def tables(self):
        self.db.sql_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [i[0] for i in self.db.sql_cursor.fetchall()]

    def get_table_columns(self, table):
        sql = f"SELECT * FROM '{table}' LIMIT 1;"
        r = self.db.sql_cursor.execute(sql)
        r.fetchone()
        return [i[0] for i in r.description]

    @timed
    def get_pairs(self, days: int = 7) -> list:
        """
        Returns an alphabetically sorted list of pairs
        (as a list of tuples) with at least one successful
        swap in the last 'x' days. ('BASE', 'REL') tuples
        are sorted by market cap to conform to CEX standards.
        """
        try:
            timestamp = int(time.time() - 86400 * days)
            sql = "SELECT COUNT(*) FROM stats_swaps;"
            self.db.sql_cursor.execute(sql)
            data = self.db.sql_cursor.fetchone()
            sql = f"SELECT DISTINCT maker_coin_ticker, maker_coin_platform, \
                    taker_coin_ticker, taker_coin_platform FROM stats_swaps \
                    WHERE finished_at > {timestamp} AND is_success=1;"
            self.db.sql_cursor.execute(sql)
            data = self.db.sql_cursor.fetchall()
            # Cover the variants
            pairs = [
                (f"{i[0]}-{i[1]}", f"{i[2]}-{i[3]}")
                for i in data
                if i[1] not in ["", "segwit"] and i[3] not in ["", "segwit"]
            ]
            pairs += [
                (f"{i[0]}-{i[1]}", f"{i[2]}")
                for i in data
                if i[1] not in ["", "segwit"]
            ]
            pairs += [
                (f"{i[0]}", f"{i[2]}-{i[3]}")
                for i in data
                if i[3] not in ["", "segwit"]
            ]
            pairs += [(f"{i[0]}", f"{i[2]}") for i in data]

            # Sort pair by ticker to expose duplicates
            sorted_pairs = set(
                [
                    order_pair_by_market_cap(
                        f"{i[0]}_{i[1]}", gecko_source=self.db.gecko_source
                    )
                    for i in pairs
                ]
            )
            sorted_pairs = [
                i for i in list(sorted_pairs) if i.split("_")[0] != i.split("_")[1]
            ]
            # Remove the duplicates
            # logger.calc(f"sorted_pairs: {len(sorted_pairs)}")
            # Sort the pair tickers with higher MC second

        except Exception as e:  # pragma: no cover
            return default_error(e)
        return list(sorted_pairs)

    @timed
    def get_swaps_for_pair(
        self,
        base: str,
        quote: str,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 100,
        start_time: int = int(time.time()) - 86400,
        end_time: int = int(time.time()),
    ) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = [i.coin for i in lib.COINS.with_segwit]
            bases = [base]
            quotes = [quote]
            if base in segwit_coins:
                bases.append(f"{base}-segwit")
            if quote in segwit_coins:
                quotes.append(f"{quote}-segwit")
            swaps_for_pair = []
            for i in bases:
                for j in quotes:
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
                    self.db.sql_cursor.execute(sql)
                    data = self.db.sql_cursor.fetchall()

                    if len(data) > 0:
                        swaps_for_pair_a_b = [dict(row) for row in data]
                    else:
                        swaps_for_pair_a_b = []

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
                    self.db.sql_cursor.execute(sql)
                    data = self.db.sql_cursor.fetchall()
                    if len(data) > 0:
                        swaps_for_pair_b_a = [dict(row) for row in data]
                    else:
                        swaps_for_pair_b_a = []

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
            return swaps_for_pair
        except sqlite3.OperationalError as e:
            return default_error(f"{e}")
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def get_swap(self, uuid):
        try:
            sql = "SELECT * FROM stats_swaps WHERE"
            sql += f" uuid='{uuid}';"
            self.db.sql_cursor.execute(sql)
            data = self.db.sql_cursor.fetchall()
            data = [dict(row) for row in data]
            if len(data) == 0:
                return {"error": f"swap uuid {uuid} not found"}
            else:
                data = data[0]
            for i in ["taker_coin_usd_price", "maker_coin_usd_price"]:
                if data[i] is None:
                    data[i] = "0"
            return data
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def get_row_count(self, table):
        try:
            self.db.sql_cursor.execute(f"SELECT COUNT(*) FROM {TablesEnum[table]}")
            r = self.db.sql_cursor.fetchone()
            return r[0]
        except Exception as e:  # pragma: no cover
            return default_error(e)

    def get_uuids(self, success_only=True, fail_only=False) -> List:
        try:
            if fail_only:
                self.db.sql_cursor.execute(
                    "SELECT uuid FROM stats_swaps WHERE is_success = 0"
                )
            elif success_only:
                self.db.sql_cursor.execute(
                    "SELECT uuid FROM stats_swaps WHERE is_success = 1"
                )
            else:
                self.db.sql_cursor.execute("SELECT uuid FROM stats_swaps")
            r = self.db.sql_cursor.fetchall()
            data = [i[0] for i in r]
            return data
        except Exception as e:  # pragma: no cover
            logger.warning(f"{e} in get_uuids with {self.db.db_path}")
            return []

    @timed
    def get_pairs_last_traded(self, started_at=None, finished_at=None, min_swaps=5):
        # TODO: Filter out test coins
        try:
            sql = "SELECT taker_coin_ticker, maker_coin_ticker, \
                    taker_coin_platform, maker_coin_platform, \
                    taker_amount AS last_taker_amount, \
                    maker_amount AS last_maker_amount, \
                    MAX(finished_at) AS last_swap, \
                    COUNT(uuid) AS swap_count, \
                    SUM(taker_amount) AS sum_taker_traded, \
                    SUM(maker_amount) AS sum_maker_traded, \
                    uuid AS last_swap_uuid \
                    FROM stats_swaps"

            sql += " WHERE is_success=1"
            if started_at is not None:
                sql += f" AND finished_at > {started_at}"
            if finished_at is not None:
                sql += f" AND finished_at < {finished_at}"
            sql += " GROUP BY taker_coin_ticker, maker_coin_ticker, \
                    taker_coin_platform, maker_coin_platform;"
            self.db.sql_cursor.execute(sql)
            resp = self.db.sql_cursor.fetchall()
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
                        if k == "last_swap":
                            item.update({k: int(v)})
                        else:
                            item.update({k: v})
                if i["taker_coin_platform"] in ["", "segwit"]:
                    taker = i["taker_coin_ticker"]
                else:
                    taker = f'{i["taker_coin_ticker"]}-{i["taker_coin_platform"]}'
                if i["maker_coin_platform"] in ["", "segwit"]:
                    maker = i["maker_coin_ticker"]
                else:
                    maker = f'{i["maker_coin_ticker"]}-{i["maker_coin_platform"]}'

                item.update(
                    {
                        "last_price": format_10f(
                            item["last_maker_amount"] / item["last_taker_amount"]
                        )
                    }
                )
                pair = f"{taker}_{maker}"
                # Handle segwit
                if pair not in by_pair_dict:
                    by_pair_dict.update({pair: item})
                elif item["last_swap"] > by_pair_dict[pair]["last_swap"]:
                    by_pair_dict.update({pair: item})
            sorted_dict = sort_dict(by_pair_dict)
            return sorted_dict
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        try:
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
            self.db.sql_cursor.execute(sql)
            resp = self.db.sql_cursor.fetchone()
            if resp is not None:
                swap_price = Decimal(resp["taker_amount"]) / Decimal(
                    resp["maker_amount"]
                )
                swap_time = int(resp["finished_at"])

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
            self.db.sql_cursor.execute(sql)
            resp2 = self.db.sql_cursor.fetchone()
            if resp2 is not None:
                swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                    resp2["taker_amount"]
                )
                swap_time2 = int(resp2["finished_at"])
            if swap_price and swap_price2:
                if swap_time > swap_time2:
                    price = swap_price
                    last_swap = swap_time
                else:
                    price = swap_price2
                    last_swap = swap_time2
            elif swap_price:
                price = swap_price
                last_swap = swap_time
            elif swap_price2:
                price = swap_price2
                last_swap = swap_time2
            else:
                price = 0
                last_swap = 0
            data = {
                "price": price,
                "timestamp": last_swap,
            }
            return data
        except Exception as e:  # pragma: no cover
            return default_error(e)

    # This was a duplicate of SqliteQuery.get_atomicdexio
    @timed
    def swap_counts(self):
        try:
            timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
            timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))
            self.db.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE is_success=1;"
            )
            swaps_all_time = self.db.sql_cursor.fetchone()[0]
            self.db.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE started_at > ? AND is_success=1;",
                (timestamp_24h_ago,),
            )
            swaps_24h = self.db.sql_cursor.fetchone()[0]
            self.db.sql_cursor.execute(
                "SELECT COUNT(*) FROM stats_swaps WHERE started_at > ? AND is_success=1;",
                (timestamp_30d_ago,),
            )
            swaps_30d = self.db.sql_cursor.fetchone()[0]
            data = {
                "swaps_all_time": swaps_all_time,
                "swaps_30d": swaps_30d,
                "swaps_24hr": swaps_24h,
            }
            return data
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
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
        try:
            tickers = []
            if end_time == 0:
                end_time = int(time.time())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = [i.coin for i in lib.COINS.with_segwit]
            if ticker in segwit_coins:
                tickers.append(f"{ticker}-segwit")

            swaps_for_ticker = []
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

                self.db.sql_cursor.execute(sql)
                data = self.db.sql_cursor.fetchall()
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
                self.db.sql_cursor.execute(sql)
                data = self.db.sql_cursor.fetchall()
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
            return data
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def get_volume_for_ticker(
        self,
        ticker: str,
        trade_type: str,
        start_time: int = int(time.time() - 86400),
        end_time: int = int(time.time()),
    ) -> list:
        """
        Returns volume traded of ticker between two timestamps.
        If no timestamp is given, returns all swaps for the ticker.
        """
        try:
            logger.info(
                f"Getting volume for {ticker} between {start_time} and {end_time}"
            )
            tickers = [ticker]
            if end_time == 0:
                end_time = int(time.time())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = [i.coin for i in lib.COINS.with_segwit]
            if ticker in segwit_coins:
                tickers.append(f"{ticker}-segwit")

            volume_for_ticker = 0
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
                    self.db.sql_cursor.execute(sql)
                    data = self.db.sql_cursor.fetchone()
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
                    self.db.sql_cursor.execute(sql)
                    data = self.db.sql_cursor.fetchone()
                    if data[0] is not None:
                        volume_as_taker = data[0]

                volume_for_ticker += volume_as_maker + volume_as_taker

            return volume_for_ticker
        except Exception as e:  # pragma: no cover
            return default_error(e)

    # Post NetId Migration below

    @timed
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
        sql = ""
        try:
            if "table" in kwargs:
                sql = f"SELECT * FROM {kwargs['table']}"
            else:
                sql = "SELECT * FROM 'stats_swaps'"
            if "sum" in kwargs and "sum_field" in kwargs == "":
                raise InvalidParamCombination(
                    "If calculating sum, you need to specify the sum_field"
                )

            if "success_only" in kwargs and "failed_only" in kwargs:
                raise InvalidParamCombination(
                    "Cant set `success_only` and `failed_only` to true at same time"
                )

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
                f" WHERE started_at > {kwargs['start']} AND finished_at < {kwargs['end']}"
            )
            if "success_only" in kwargs:
                sql += " AND is_success=1"
            elif "failed_only" in kwargs:
                sql += " AND is_success=0"
            if "filter_sql" in kwargs:
                sql += kwargs["filter_sql"].replace("WHERE", "AND")
            logger.info(sql)
            return sql
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def get_timespan_swaps(self, **kwargs) -> list:
        """
        Returns a list of swaps between two timestamps
        """
        try:
            sql = self.build_query(**kwargs)
            self.db.sql_cursor.execute(sql)
            data = self.db.sql_cursor.fetchall()
        except Exception as e:  # pragma: no cover
            return default_error(e)
        msg = f"{len(data)} swaps for netid {self.netid}"
        logger.query(msg)
        return data


class SqliteUpdate:  # pragma: no cover
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
            self.files = Files(testing=self.testing)
            self.db = db
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)}: Failed to init SqliteQuery: {e}")
            return

    @timed
    def remove_overlaps(self, remove_db):
        try:
            uuids = self.db.query.get_uuids()
            uuids_remove = remove_db.query.get_uuids(success_only=False)
            overlap = set(uuids_remove).intersection(set(uuids))
            if len(overlap) > 0:
                remove_db.update.remove_uuids(overlap)
                msg = f"{len(overlap)} uuids removed from {remove_db.db_path}"
            else:
                msg = f"No UUIDs to remove from {remove_db.db_path}"
            return default_result(msg=msg, loglevel="updated")
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Failed to remove UUIDs from {remove_db.db_path}: {e}"
            return default_error(e, msg=msg)

    @timed
    def update_stats_swap_row(self, uuid, data):
        try:
            cols = ", ".join([f"{k} = ?" for k in data.keys()])
            colvals = tuple(data.values()) + (uuid,)
            # logger.calc(colvals)
            t = colvals
            sql = f"UPDATE 'stats_swaps' SET {cols} WHERE uuid = ?;"
            # logger.calc(sql)
            self.db.sql_cursor.execute(sql, t)
            self.db.conn.commit()
            return default_result(msg=f"{uuid} updated in {self.db.db_file}")
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default_error(e)
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def clear(self, table):
        try:
            self.db.sql_cursor.execute(f"DELETE FROM {table};")
            self.db.conn.commit()
            return
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default_error(e)
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def create_swap_stats_table(self):
        try:
            self.db.sql_cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS
                stats_swaps (
                    id INTEGER NOT NULL PRIMARY KEY,
                    maker_coin VARCHAR(255) NOT NULL,
                    taker_coin VARCHAR(255) NOT NULL,
                    uuid VARCHAR(255) NOT NULL UNIQUE,
                    started_at INTEGER NOT NULL,
                    finished_at INTEGER NOT NULL,
                    maker_amount DECIMAL NOT NULL,
                    taker_amount DECIMAL NOT NULL,
                    is_success INTEGER NOT NULL,
                    maker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                    maker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                    taker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                    taker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                    maker_coin_usd_price DECIMAL NOT NULL DEFAULT 0,
                    taker_coin_usd_price DECIMAL NOT NULL DEFAULT 0,
                    maker_pubkey VARCHAR(255) NOT NULL DEFAULT '',
                    taker_pubkey VARCHAR(255) NOT NULL DEFAULT ''
                );
                """
            )
            msg = f"'stats_swaps' table created for {self.db.db_path}"
            return default_result(msg=msg, loglevel="muted")
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default_error(e)
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def remove_uuids(self, remove_list: set(), table: str = "stats_swaps") -> None:
        try:
            remove_list = list(remove_list)
            sql = f"DELETE FROM {TablesEnum[table]}"
            if len(remove_list) == 1:
                remove_list = remove_list[0]
                sql += " WHERE uuid = ?;"
                t = (remove_list,)
                self.db.sql_cursor.execute(sql, t)
            else:
                uuids = ", ".join(f"'{i}'" for i in remove_list)
                sql += f" WHERE uuid IN ({uuids});"
                self.db.sql_cursor.execute(sql)
            self.db.conn.commit()
            return
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default_error(e, sql)
        except Exception as e:  # pragma: no cover
            return default_error(e, sql)

    @timed
    def denullify_stats_swaps(self):
        columns = [
            "maker_coin_usd_price",
            "taker_coin_usd_price",
            "maker_pubkey",
            "taker_pubkey",
        ]
        for column in columns:
            try:
                if column in ["maker_coin_usd_price", "taker_coin_usd_price"]:
                    value = 0
                if column in ["maker_pubkey", "taker_pubkey"]:
                    value = "''"
                self.denullify_table_column("stats_swaps", column, value)
            except sqlite3.OperationalError as e:  # pragma: no cover
                msg = f"{type(e)} for {self.db.db_path}: {e}"
                return default_error(e, msg)
            except Exception as e:  # pragma: no cover
                msg = f"{type(e)} for {self.db.db_path}: {e}"
                return default_error(e, msg)
        return default_result(
            msg=f"Nullification of {len(columns)} columns in {self.db.db_file} complete!",
            loglevel="updated",
            ignore_until=10,
        )

    @timed
    def denullify_table_column(self, table, column, value="''"):
        try:
            t = (value,)
            sql = f"UPDATE {TablesEnum[table]}"
            sql += f" SET {ColumnsEnum[column]} = ?"
            sql += f" WHERE {ColumnsEnum[column]} IS NULL"
            self.db.sql_cursor.execute(
                sql,
                t,
            )
            self.db.conn.commit()
            return default_result(
                msg=f"Nullification of {column} in {self.db.db_file} complete!",
                loglevel="updated",
                ignore_until=10,
            )
        except sqlite3.OperationalError as e:  # pragma: no cover
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default_error(e, msg)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default_error(e, msg)


def get_sqlite_db(
    db_path=None, testing: bool = False, netid=None, db=None, **kwargs
):  # pragma: no cover
    if db is not None:
        return db
    if netid is not None:
        db_path = get_sqlite_db_paths(netid)
    if db_path is None:
        logger.warning("DB path is none")
    db = SqliteDB(db_path=db_path, testing=testing, **kwargs)
    # logger.info(f"Connected to DB [{db.db_path}]")
    return db


def get_sqlite_db_paths(netid=MM2_NETID):
    return MM2_DB_PATHS[str(netid)]


def list_sqlite_dbs(folder):
    db_list = [i for i in os.listdir(folder) if i.endswith(".db")]
    db_list.sort()
    return db_list


def view_locks(cursor):  # pragma: no cover
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()


def is_source_db(db_file: str) -> bool:
    if db_file.endswith("MM2.db"):
        return True
    return False


def is_7777(db_file: str) -> bool:
    if db_file.startswith("seed"):
        return True
    return False


def get_netid(db_file):
    for netid in NetId:
        if netid.value in db_file:
            return netid.value
    if is_7777(db_file):
        return "7777"
    elif is_source_db(db_file=db_file):
        return "8762"
    else:
        return "ALL"


def compare_uuid_fields(swap1, swap2):
    uuid = swap1["uuid"]
    # logger.muted(f"Repairing swap {uuid}")
    try:
        fixed = {}
        for k, v in swap1.items():
            if k in compare_fields:
                if v != swap2[k]:
                    # use higher value for below fields
                    try:
                        fixed.update({k: str(max([Decimal(v), Decimal(swap2[k])]))})
                    except sqlite3.OperationalError as e:  # pragma: no cover
                        msg = f"{uuid} | {v} vs {swap2[k]} | {type(v)} vs {type(swap2[k])}"
                        return default_error(e, msg)
        return fixed
    except Exception as e:  # pragma: no cover
        return default_error(e)
