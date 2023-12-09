#!/usr/bin/env python3
import os
import time
import sqlite3
from decimal import Decimal
from datetime import datetime, timedelta
from logger import logger
from helper import order_pair_by_market_cap, get_sqlite_db_paths
from generics import Files
from utils import Utils
from enums import TradeType
from const import (
    PROJECT_ROOT_PATH,
    LOCAL_MM2_DB_PATH_7777,
    LOCAL_MM2_DB_PATH_8762,
    LOCAL_MM2_DB_BACKUP_7777,
    LOCAL_MM2_DB_BACKUP_8762,
    MM2_DB_PATHS,
)


def get_sqlite_db(
    path_to_db=None, testing: bool = False, DB=None, dict_format=False, netid=None
):
    if DB is not None:
        return DB

    if netid is not None:
        path_to_db = get_sqlite_db_paths(netid)
    db = SqliteDB(path_to_db=path_to_db, testing=testing, dict_format=dict_format)
    # logger.info(f"Connected to DB [{db.path_to_db}]")
    return db


class SqliteDB:
    def __init__(self, path_to_db, dict_format=False, testing: bool = False):
        self.testing = testing
        self.utils = Utils(testing=self.testing)
        self.files = Files(testing=self.testing)
        self.path_to_db = path_to_db
        self.conn = self.connect()
        if dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        self.gecko_source = self.utils.load_jsonfile(self.files.gecko_source_file)

    def close(self):
        self.conn.close()
        # logger.info(f"Closed connection to {self.path_to_db}")

    def connect(self):
        return sqlite3.connect(self.path_to_db)

    def import_swap_stats_data(self, src_db_path, table, column):
        sql = ""
        try:
            if src_db_path != self.path_to_db:
                self.denullify_stats_swaps()
                try:
                    src_db = get_sqlite_db(path_to_db=src_db_path)
                    src_db.denullify_stats_swaps()
                except Exception as e:
                    err = {
                        "Error": f"{type(e)} in [import_swap_data] {e}",
                        "db": src_db_path,
                    }
                    logger.warning(err)
                    return

                src_columns = src_db.get_table_columns(table)
                src_columns.pop(src_columns.index("id"))
                sql = f"ATTACH DATABASE '{src_db_path}' AS src_db;"
                sql += f" INSERT INTO {table} ({','.join(src_columns)})"
                sql += f" SELECT {','.join(src_columns)}"
                sql += f" FROM src_db.{table}"
                sql += " WHERE NOT EXISTS ("
                sql += f"SELECT * FROM {table}"
                sql += f" WHERE {table}.{column} = src_db.{table}.{column});"
                sql += " DETACH DATABASE 'src_db';"
                self.sql_cursor.executescript(sql)

        except Exception as e:
            logger.warning(
                {
                    "Error": str(e),
                    "src_db": src_db_path,
                    "dest_db": self.path_to_db,
                    "sql": sql,
                }
            )

    def denullify_stats_swaps(self):
        for column in [
            "maker_coin_usd_price",
            "taker_coin_usd_price",
            "maker_pubkey",
            "taker_pubkey",
        ]:
            self.denullify_db("stats_swaps", column)
        # logger.debug(f"Nullified 'stats_swaps' for {self.path_to_db}")

    def denullify_db(self, table, column, value="''"):
        if column in ["maker_coin_usd_price", "taker_coin_usd_price"]:
            value = 0
        if column in ["maker_pubkey", "taker_pubkey"]:
            value = "''"
        sql = f"UPDATE {table} SET {column} = {value} WHERE {column} IS NULL"
        self.sql_cursor.execute(sql)
        self.conn.commit()

    def update_stats_swap_row(self, uuid, data):
        colvals = ",".join([f"{k} = {v}" for k, v in data.items()])
        sql = f"UPDATE {'stats_swaps'} SET {colvals} WHERE uuid = '{uuid}';"
        self.sql_cursor.execute(sql)
        self.conn.commit()

    @property
    def tables(self):
        self.sql_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [i[0] for i in self.sql_cursor.fetchall()]

    def get_table_columns(self, table):
        sql = f"SELECT * FROM '{table}' LIMIT 1;"
        r = self.sql_cursor.execute(sql)
        r.fetchone()
        return [i[0] for i in r.description]

    def get_pairs(self, days: int = 7) -> list:
        """
        Returns an alphabetically sorted list of pairs
        (as a list of tuples) with at least one successful
        swap in the last 'x' days. ('BASE', 'REL') tuples
        are sorted by market cap to conform to CEX standards.
        """
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
        # Sort pair by ticker to expose base-rel and rel-base duplicates
        sorted_pairs = [tuple(sorted(pair)) for pair in pairs]
        # Remove the duplicates
        pairs = list(set(sorted_pairs))
        # Sort the pair tickers with lower MC first and higher MC second
        data = sorted(
            [order_pair_by_market_cap(pair, self.gecko_source) for pair in pairs]
        )
        return data

    def get_swaps_for_pair(
        self,
        pair: tuple,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 0,
        start_time: int = 0,
        end_time: int = 0,
    ) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        # logger.warning(pair)
        try:
            pair = order_pair_by_market_cap(pair, self.gecko_source)
            base = [pair[0]]
            quote = [pair[1]]
            if end_time == 0:
                end_time = int(time.time())

            # We stripped segwit from the pairs in get_pairs()
            # so we need to add it back here if it's present
            segwit_coins = self.utils.segwit_coins()
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
            return swaps_for_pair

        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
            return []

    def get_swap(self, uuid):
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
        return data

    def get_row_count(self, table):
        self.sql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        r = self.sql_cursor.fetchone()
        return r[0]

    def get_uuids(self, success_only=True, fail_only=False):
        if fail_only:
            self.sql_cursor.execute("SELECT uuid FROM stats_swaps WHERE is_success = 0")
        elif success_only:
            self.sql_cursor.execute("SELECT uuid FROM stats_swaps WHERE is_success = 1")
        else:
            self.sql_cursor.execute("SELECT uuid FROM stats_swaps")
        r = self.sql_cursor.fetchall()
        return [i[0] for i in r]

    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        swap_price = None
        swap_time = None
        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{base.split('-')[0]}' \
                AND taker_coin_ticker='{quote.split('-')[0]}' AND is_success=1"
        if len(base.split("-")) == 2:
            platform = base.split("-")[1]
            sql += f" AND maker_coin_platform='{platform}'"
        if len(quote.split("-")) == 2:
            platform = quote.split("-")[1]
            sql += f" AND taker_coin_platform='{platform}'"
        sql += " ORDER BY finished_at DESC LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp = self.sql_cursor.fetchone()
        if resp is not None:
            swap_price = Decimal(resp["taker_amount"]) / Decimal(resp["maker_amount"])
            swap_time = resp["finished_at"]

        swap_price2 = None
        swap_time2 = None
        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{quote.split('-')[0]}' \
                AND taker_coin_ticker='{base.split('-')[0]}' AND is_success=1"
        if len(base.split("-")) == 2:
            platform = base.split("-")[1]
            sql += f" AND taker_coin_platform='{platform}'"
        if len(quote.split("-")) == 2:
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
        return {
            "price": price,
            "timestamp": last_swap_time,
        }

    def create_swap_stats_table(self):
        self.sql_cursor.execute(
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

    def remove_uuids(self, remove_list):
        # rows = self.get_row_count("stats_swaps")
        # logger.debug(f"{rows} before removing overlaps")
        sql = f"DELETE FROM stats_swaps WHERE uuid in {tuple(remove_list)}"

        self.sql_cursor.execute(sql)
        self.conn.commit()
        # rows = self.get_row_count("stats_swaps")
        # logger.debug(f"{rows} after overlaps removed")

    def swap_counts(self):
        timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
        timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))

        self.sql_cursor
        self.sql_cursor.execute("SELECT COUNT(*) FROM stats_swaps WHERE is_success=1;")
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
        return {
            "swaps_all_time": swaps_all_time,
            "swaps_30d": swaps_30d,
            "swaps_24h": swaps_24h,
        }

    def get_swaps_for_ticker(
        self,
        ticker: str,
        trade_type: TradeType = TradeType.ALL,
        limit: int = 0,
        start_time: int = 0,
        end_time: int = 0,
    ) -> list:
        """
        Returns a list of swaps for a given ticker between two timestamps.
        If no timestamp is given, returns all swaps for the ticker.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
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
            swaps_for_pair = sorted(
                swaps_for_ticker, key=lambda k: k["finished_at"], reverse=True
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

        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
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
            return volume_for_ticker

        except Exception as e:  # pragma: no cover
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
            return 0


def list_sqlite_dbs(folder):
    return [i for i in os.listdir(folder) if i.endswith(".db")]


def progress(status, remaining, total, show=False):
    if show:
        logger.debug(f"Copied {total-remaining} of {total} pages...")


def backup_db(src_db_path, dest_db_path):
    src = get_sqlite_db(path_to_db=src_db_path)
    dest = get_sqlite_db(path_to_db=dest_db_path)
    src.conn.backup(dest.conn, pages=1, progress=progress)
    # logger.debug(f'Backed up {src_db_path} to {dest_db_path}')
    dest.close()
    src.close()


def init_dbs():
    logger.debug("Initialising databases...")
    for i in MM2_DB_PATHS:
        db = get_sqlite_db(path_to_db=MM2_DB_PATHS[i])
        db.create_swap_stats_table()
        db.close()
    for i in [
        LOCAL_MM2_DB_BACKUP_7777,
        LOCAL_MM2_DB_PATH_7777,
        LOCAL_MM2_DB_BACKUP_8762,
        LOCAL_MM2_DB_PATH_8762,
    ]:
        db = get_sqlite_db(path_to_db=i)
        db.create_swap_stats_table()
        db.close()


def backup_local_dbs():
    try:
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_7777, dest_db_path=LOCAL_MM2_DB_BACKUP_7777
        )
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_8762, dest_db_path=LOCAL_MM2_DB_BACKUP_8762
        )
        return {"result": "backed up local databases"}
    except Exception as e:
        err = f"Error in [backup_local_dbs]: {e}"
        logger.warning(err)
        return {"error": err}


def update_master_sqlite_dbs():
    backup_local_dbs()

    # Merge local into master netid dbs
    db_7777 = get_sqlite_db(path_to_db=MM2_DB_PATHS["7777"])
    db_7777.import_swap_stats_data(
        src_db_path=LOCAL_MM2_DB_BACKUP_7777, table="stats_swaps", column="uuid"
    )
    db_7777.close()

    db_8762 = get_sqlite_db(path_to_db=MM2_DB_PATHS["8762"])
    db_8762.import_swap_stats_data(
        src_db_path=LOCAL_MM2_DB_BACKUP_8762, table="stats_swaps", column="uuid"
    )
    db_8762.close()

    # Get list of supplemental db files
    db_folder = f"{PROJECT_ROOT_PATH}/DB"
    sqlite_db_list = list_sqlite_dbs(db_folder)
    for source_db_file in sqlite_db_list:
        source_db_path = f"{db_folder}/{source_db_file}"

        if source_db_file.startswith("seed"):
            db_7777 = get_sqlite_db(path_to_db=MM2_DB_PATHS["7777"])
            db_7777.import_swap_stats_data(
                src_db_path=source_db_path, table="stats_swaps", column="uuid"
            )
            db_7777.close()
        elif source_db_file.startswith("streamseed"):
            db_8762 = get_sqlite_db(path_to_db=MM2_DB_PATHS["8762"])
            db_8762.import_swap_stats_data(
                src_db_path=source_db_path, table="stats_swaps", column="uuid"
            )
            db_8762.close()

    for i in ["7777", "8762"]:
        db_all = get_sqlite_db(path_to_db=MM2_DB_PATHS["all"])
        db_all.import_swap_stats_data(
            src_db_path=MM2_DB_PATHS[i], table="stats_swaps", column="uuid"
        )
        db_all.close()
    remove_overlaps()
    return {"result": "merge to master databases complete"}


def remove_overlaps():
    # Remove from 8762 if in 7777
    db_8762 = get_sqlite_db(path_to_db=MM2_DB_PATHS["8762"])
    db_7777 = get_sqlite_db(path_to_db=MM2_DB_PATHS["7777"])
    db_all = get_sqlite_db(path_to_db=MM2_DB_PATHS["all"])

    uuids_7777 = db_7777.get_uuids()
    uuids_8762 = db_8762.get_uuids()
    overlap = set(uuids_8762).intersection(set(uuids_7777))
    if len(overlap) > 0:
        db_8762.remove_uuids(overlap)

    uuids_7777 = db_7777.get_uuids()
    uuids_8762 = db_8762.get_uuids()
    overlap = set(uuids_7777).intersection(set(uuids_8762))
    if len(overlap) > 0:
        db_7777.remove_uuids(overlap)

    uuids_7777 = db_7777.get_uuids()
    uuids_8762 = db_8762.get_uuids()
    uuids_all = db_all.get_uuids()
    inspect = set(uuids_all) - set(uuids_7777) - set(uuids_8762)
    uuids_7777 = db_7777.get_uuids()
    uuids_8762 = db_8762.get_uuids()
    uuids_all = db_all.get_uuids()
    inspect = set(list(uuids_7777) + list(uuids_8762)) - set(uuids_all)
    for i in list(inspect):
        try:
            swap_7777 = db_7777.get_swap(i)
            swap_8762 = db_8762.get_swap(i)
            fixed = {}
            for k, v in swap_7777.items():
                if k != "id":
                    if k in swap_8762:
                        if swap_8762[k] != v:
                            logger.debug(
                                f"UUID [{i}] duplicate mismatch for {k}: {v} vs {swap_8762[k]}"
                            )
                            if k in [
                                "is_success",
                                "started_at",
                                "finished_at",
                                "maker_coin_usd_price",
                                "taker_coin_usd_price",
                            ]:
                                fixed.update({k: max([v, swap_8762[k]])})
                    else:
                        raise Exception
            if len(fixed) > 0:
                db_7777.update_stats_swap_row(i, fixed)
                db_8762.update_stats_swap_row(i, fixed)
                db_all.update_stats_swap_row(i, fixed)
        except Exception as e:
            logger.error(f"Failed to repair swap [{i}]: {e}")
            logger.debug(f"swap_7777: {swap_7777}")
            logger.debug(f"swap_8762: {swap_8762}")
            time.sleep(5)

    db_7777.close()
    db_8762.close()
    db_all.close()


def view_locks(cursor):
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()


init_dbs()
