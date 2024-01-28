#!/usr/bin/env python3
from os.path import basename
import util.cron as cron
import sqlite3
from typing import List
from datetime import datetime, timedelta
from const import MM2_DB_PATHS, MM2_NETID
from lib.coins import (
    pair_without_segwit_suffix,
    get_segwit_coins,
)

from util.enums import TradeType, TablesEnum, NetId, ColumnsEnum
from util.exceptions import InvalidParamCombination
from util.files import Files
from util.logger import logger, timed
from util.transform import sortdata
import util.defaults as default
import util.helper as helper
import util.transform as transform
import util.validate as validate


class SqliteDB:  # pragma: no cover
    def __init__(self, db_path, **kwargs):
        try:
            self.kwargs = kwargs
            self.start = int(cron.now_utc())
            self.db_path = db_path
            self.db_file = basename(self.db_path)
            self.options = [
                "wal",
            ]
            default.params(self, self.kwargs, self.options)
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
            logger.error(f"{type(e)}: Failed to init SqliteDB {self.db_path}: {e}")

    @property
    def netid(self):
        for netid in NetId:
            if netid.value in self.db_file:
                return netid.value
        if validate.is_7777("seed"):
            return "7777"
        elif validate.is_source_db("MM2.db"):
            return "8762"
        else:
            return "ALL"

    @timed
    def close(self):
        self.conn.close()
        msg = f"Connection to {self.db_file} closed"
        return default.result(msg=msg, loglevel="debug", ignore_until=10)

    def connect(self):
        return sqlite3.connect(self.db_path)

    def truncate_wal(self):  # pragma: no cover
        sql = "PRAGMA wal_checkpoint(truncate);"
        self.sql_cursor.execute(sql)


class SqliteQuery:  # pragma: no cover
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.segwit_coins = [i for i in get_segwit_coins()]
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
    def get_swaps_for_pair_old(
        self,
        base: str,
        quote: str,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 100,
        start_time: int = int(cron.now_utc()) - 86400,
        end_time: int = int(cron.now_utc()),
    ) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            bases = [base]
            quotes = [quote]
            if base in self.segwit_coins and "-" not in base:
                bases.append(f"{base}-segwit")
            if quote in self.segwit_coins and "-" not in base:
                quotes.append(f"{quote}-segwit")
            swaps_for_pair = []
            for i in bases:
                for j in quotes:
                    # Get base = maker; quote = taker
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

                    # Get base = taker; quote = maker
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
            return default.error(f"{e}")
        except Exception as e:  # pragma: no cover
            return default.error(e)

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
            return data
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def get_row_count(self, table):
        try:
            self.db.sql_cursor.execute(f"SELECT COUNT(*) FROM {TablesEnum[table]}")
            r = self.db.sql_cursor.fetchone()
            return r[0]
        except Exception as e:  # pragma: no cover
            return default.error(e)

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
    def get_pairs_last_traded(self):
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
            sql += " GROUP BY taker_coin_ticker, maker_coin_ticker, \
                    taker_coin_platform, maker_coin_platform;"
            self.db.sql_cursor.execute(sql)
            resp = self.db.sql_cursor.fetchall()
            resp = [dict(i) for i in resp]
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

                pair = pair_without_segwit_suffix(
                    maker_coin=f'{i["maker_coin_ticker"]}-{i["maker_coin_platform"]}',
                    taker_coin=f'{i["taker_coin_ticker"]}-{i["taker_coin_platform"]}',
                )
                std_pair = sortdata.order_pair_by_market_cap(pair)
                last_price = item["last_maker_amount"] / item["last_taker_amount"]
                sum_maker = item["sum_maker_traded"]
                sum_taker = item["sum_taker_traded"]
                last_taker_amount = item["last_taker_amount"]
                last_maker_amount = item["last_maker_amount"]
                item.update({"last_swap_price": transform.format_10f(last_price)})
                swap_count = item["swap_count"]
                last_uuid = item["last_swap_uuid"]
                last_swap = item["last_swap"]

                # Handle segwit
                if std_pair not in by_pair_dict:
                    by_pair_dict.update({std_pair: item})
                elif last_swap > by_pair_dict[std_pair]["last_swap"]:
                    by_pair_dict[std_pair]["last_maker_amount"] = last_maker_amount
                    by_pair_dict[std_pair]["last_taker_amount"] = last_taker_amount
                    by_pair_dict[std_pair]["last_swap"] = last_swap
                    by_pair_dict[std_pair]["last_swap_uuid"] = last_uuid
                    by_pair_dict[std_pair]["sum_maker_traded"] += sum_maker
                    by_pair_dict[std_pair]["sum_taker_traded"] += sum_taker
                    by_pair_dict[std_pair]["swap_count"] += swap_count

            sorted_dict = sortdata.sort_dict(by_pair_dict)
            return sorted_dict
        except Exception as e:  # pragma: no cover
            return default.error(e)

    # This was a duplicate of SqliteQuery.get_atomicdexio
    @timed
    def swap_counts_old(self):
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
            return default.error(e)

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

            sql += f" WHERE started_at > {kwargs['start']} AND finished_at < {kwargs['end']}"
            if "success_only" in kwargs:
                sql += " AND is_success=1"
            elif "failed_only" in kwargs:
                sql += " AND is_success=0"
            if "filter_sql" in kwargs:
                sql += kwargs["filter_sql"].replace("WHERE", "AND")
            return sql
        except Exception as e:  # pragma: no cover
            return default.error(e)

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
            return default.error(e)
        return data

    def last_24h_swaps(self):
        sql = "SELECT * FROM stats_swaps "
        sql += "WHERE finished_at > ? AND finished_at < ? AND is_success=1;"
        data = self.db.sql_cursor.execute(
            sql,
            (int(cron.now_utc()) - 86400, int(cron.now_utc())),
        )
        resp = [dict(row) for row in data]
        return resp

    @timed
    def get_swaps_for_coin(
        self,
        coin: str,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 100,
        start_time: int = int(cron.now_utc()) - 86400,
        end_time: int = 0,
    ) -> list:
        """
        Returns a list of swaps for a given coin between two timestamps.
        If no timestamp is given, returns all swaps for the coin.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            coin = coin.split("-")[0]
            variants = helper.get_coin_variants(coin)
            if end_time == 0:
                end_time = int(cron.now_utc())
            resp = {}
            swaps = []
            for i in variants:
                coin_ticker = i.split("-")[0]
                coin_platform = ""
                if len(i.split("-")) == 2:
                    coin_platform = i.split("-")[1]

                # As Maker
                sql = "SELECT * FROM stats_swaps WHERE"
                sql += f" finished_at > {start_time}"
                sql += f" AND finished_at < {end_time}"
                sql += f" AND maker_coin_ticker='{coin_ticker}'"
                sql += " AND is_success=1 ORDER BY finished_at DESC"
                if limit > 0:
                    sql += f" LIMIT {limit}"
                sql += ";"

                self.db.sql_cursor.execute(sql)
                data = self.db.sql_cursor.fetchall()
                swaps_as_maker = [dict(row) for row in data]
                for swap in swaps_as_maker:
                    swap["trade_type"] = "sell"

                # As Taker
                sql = "SELECT * FROM stats_swaps WHERE"
                sql += f" finished_at > {start_time}"
                sql += f" AND finished_at < {end_time}"
                sql += f" AND taker_coin_ticker='{coin_ticker}'"
                sql += " AND is_success=1 ORDER BY finished_at DESC"
                if limit > 0:
                    sql += f" LIMIT {limit}"
                sql += ";"

                self.db.sql_cursor.execute(sql)
                data = self.db.sql_cursor.fetchall()
                swaps_as_taker = [dict(row) for row in data]
                for swap in swaps_as_taker:
                    swap["trade_type"] = "buy"

                swaps += swaps_as_maker + swaps_as_taker

                if coin_platform != "":
                    pair_str = f"{coin_ticker}-{coin_platform}"
                else:
                    pair_str = f"{coin_ticker}"
                resp.update({pair_str: swaps_as_maker + swaps_as_taker})

            resp.update({f"{coin_ticker}-ALL": swaps})
            for i in resp:
                # Sort swaps by timestamp
                data = sorted(resp[i], key=lambda k: k["finished_at"], reverse=True)
                if trade_type == TradeType.BUY:
                    resp[i] = [swap for swap in data if swap["trade_type"] == "buy"]
                elif trade_type == TradeType.SELL:
                    resp[i] = [swap for swap in data if swap["trade_type"] == "sell"]
                if limit > 0:
                    resp[i] = resp[i][:limit]

            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "range_days": (end_time - start_time) / 86400,
                "trade_type": trade_type,
                "data": resp,
            }
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def get_volume_for_coin(
        self,
        coin: str,
        trade_type: TradeType = TradeType.ALL,
        start_time: int = int(cron.now_utc() - 86400),
        end_time: int = int(cron.now_utc()),
    ) -> list:
        """
        Returns volume traded of coin between two timestamps.
        If no timestamp is given, returns all swaps for the coin.
        """
        try:
            resp = {}
            coin = coin.split("-")[0]
            variants = helper.get_coin_variants(coin)
            if end_time == 0:
                end_time = int(cron.now_utc())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present

            volume_for_coin = 0
            for i in variants:
                coin_ticker = i.split("-")[0]
                coin_platform = ""
                if len(i.split("-")) == 2:
                    coin_platform = i.split("-")[1]

                volume_as_maker = 0
                if trade_type in [TradeType.BUY, TradeType.ALL]:
                    sql = "SELECT SUM(CAST(maker_amount AS NUMERIC)) FROM stats_swaps WHERE"
                    sql += f" finished_at > {start_time}"
                    sql += f" AND finished_at < {end_time}"
                    sql += f" AND maker_coin_ticker='{coin_ticker}'"
                    sql += f" AND maker_coin_platform='{coin_platform}'"
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
                    sql += f" AND taker_coin_ticker='{coin_ticker}'"
                    sql += f" AND taker_coin_platform='{coin_platform}'"
                    sql += " AND is_success=1 ORDER BY finished_at DESC;"
                    self.db.sql_cursor.execute(sql)
                    data = self.db.sql_cursor.fetchone()
                    if data[0] is not None:
                        volume_as_taker = data[0]

                volume_for_coin += volume_as_maker + volume_as_taker
                if coin_platform != "":
                    pair_str = f"{coin_ticker}-{coin_platform}"
                else:
                    pair_str = f"{coin_ticker}"
                resp.update({pair_str: volume_as_maker + volume_as_taker})

            resp.update({f"{coin_ticker}-ALL": volume_for_coin})
            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "range_days": (end_time - start_time) / 86400,
                "trade_type": trade_type,
                "data": resp,
            }
        except Exception as e:  # pragma: no cover
            return default.error(e)

    # Post NetId Migration below


class SqliteUpdate:  # pragma: no cover
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.files = Files()
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
            return default.result(msg=msg, loglevel="updated")
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Failed to remove UUIDs from {remove_db.db_path}: {e}"
            return default.error(e, msg=msg)

    @timed
    def update_stats_swap_row(self, uuid, data):
        try:
            cols = ", ".join([f"{k} = ?" for k in data.keys()])
            colvals = tuple(data.values()) + (uuid,)
            t = colvals
            sql = f"UPDATE 'stats_swaps' SET {cols} WHERE uuid = ?;"
            self.db.sql_cursor.execute(sql, t)
            self.db.conn.commit()
            return default.result(msg=f"{uuid} updated in {self.db.db_file}")
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default.error(e)
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def clear(self, table):
        try:
            self.db.sql_cursor.execute(f"DELETE FROM {table};")
            self.db.conn.commit()
            return
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default.error(e)
        except Exception as e:  # pragma: no cover
            return default.error(e)

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
            return default.result(msg=msg, loglevel="muted")
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default.error(e)
        except Exception as e:  # pragma: no cover
            return default.error(e)

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
            return default.error(e, sql)
        except Exception as e:  # pragma: no cover
            return default.error(e, sql)

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
                return default.error(e, msg)
            except Exception as e:  # pragma: no cover
                msg = f"{type(e)} for {self.db.db_path}: {e}"
                return default.error(e, msg)
        return default.result(
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
            return default.result(
                msg=f"Nullification of {column} in {self.db.db_file} complete!",
                loglevel="updated",
                ignore_until=10,
            )
        except sqlite3.OperationalError as e:  # pragma: no cover
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default.error(e, msg)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default.error(e, msg)


# TODO: Move to sqlite_merge
def get_sqlite_db(db_path=None, netid=None, db=None, **kwargs):  # pragma: no cover
    if db is not None:
        return db
    if netid is not None:
        db_path = get_sqlite_db_paths(netid)
    if db_path is None:
        logger.warning("DB path is none")
    db = SqliteDB(db_path=db_path, **kwargs)
    # logger.info(f"Connected to DB [{db.db_path}]")
    return db


# TODO: Move to sqlite_merge
def get_sqlite_db_paths(netid=MM2_NETID):
    return MM2_DB_PATHS[str(netid)]


def view_locks(cursor):  # pragma: no cover
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()
