#!/usr/bin/env python3
import time
import sqlite3
import inspect
from random import randrange
from db.sqlitedb_query import SqliteQuery
from util.files import Files
from util.utils import Utils
from const import templates
from util.helper import get_netid
from util.logger import logger, get_trace, StopWatch, timed
from util.templates import default_error, default_result



class SqliteUpdate:
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing"]
            templates.set_params(self, self.kwargs, self.options)
            self.utils = Utils(testing=self.testing)
            self.files = Files(testing=self.testing)
            self.db = db
            self.db_path = db.db_path
            self.db_file = db.db_file
            self.netid = get_netid(self.db_file)
            self.conn = db.conn
            self.sql_cursor = self.conn.cursor()
            self.query = SqliteQuery(db=self.db)
        except Exception as e:
            error = f"{type(e)}: Failed to init SqliteQuery: {e}"
            return

    def merge_db_tables(self, src_db, table, column, since=None):
        if since is None:
            since = int(time.time()) - 86400 * 7
        sql = ""
        n = 0
        while True:
            try:
                src_db_query = SqliteQuery(db = src_db)
                src_columns = src_db_query.get_table_columns(table)
                src_columns.pop(src_columns.index("id"))
                context = f"{src_db.db_path} ==> {self.db_path}"
                sql = f"ATTACH DATABASE '{src_db.db_path}' AS src_db;"
                sql += f" INSERT INTO {table} ({','.join(src_columns)})"
                sql += f" SELECT {','.join(src_columns)}"
                sql += f" FROM src_db.{table}"
                sql += " WHERE NOT EXISTS ("
                sql += f"SELECT * FROM {table}"
                sql += f" WHERE {table}.{column} = src_db.{table}.{column})"
                sql += f" AND src_db.{table}.finished_at > {since};"
                sql += " DETACH DATABASE 'src_db';"
                self.db.sql_cursor.executescript(sql)
                msg = f"OK for {src_db.db_path} ==> {self.db_path}"
                return default_error
            except sqlite3.OperationalError as e:
                msg = f"OpErr {src_db.db_path} ==> {self.db_path}"
                return default_error
            except Exception as e:
                msg = f"{type(e)} {src_db.db_path} ==> {self.db_path} {e}"
                time.sleep(2)
                return default_error

    def remove_overlaps(self, remove_db):
        uuids = self.query.get_uuids()
        query_remove_db = SqliteQuery(db=remove_db)
        uuids_remove = query_remove_db.get_uuids(success_only=False)
        overlap = set(uuids_remove).intersection(set(uuids))
        if len(overlap) > 0:
            remove_db.remove_uuids(overlap)

    def update_stats_swap_row(self, uuid, data):
        n = 0
        while True:
            try:
                colvals = ",".join([f"{k} = {v}" for k, v in data.items()])
                sql = f"UPDATE {'stats_swaps'} SET {colvals} WHERE uuid = '{uuid}';"
                return
            except sqlite3.OperationalError as e:
                logger.warning(f"Error in [update_stats_swap_row]: {e}...")
            except Exception as e:
                logger.error(f"Error in [update_stats_swap_row]: {e}")
                return

    def clear(self, table):
        n = 0
        while True:
            try:
                self.db.sql_cursor.execute(f"DELETE FROM {table}")
                self.db.conn.commit()
                return
            except sqlite3.OperationalError as e:
                return
            except Exception as e:  # pragma: no cover
                return

    def create_swap_stats_table(self):
        n = 0
        while True:
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
                return
            except sqlite3.OperationalError as e:
                msg = f"{type(e)} Error in [create_swap_stats_table]: {e}"
                return default_error(e, msg)
            except Exception as e:  # pragma: no cover
                msg = f"{type(e)} Error in [create_swap_stats_table]: {e}"
                return default_error(e, msg)

    def remove_uuids(self, remove_list: set(), table: str = "stats_swaps") -> None:
        n = 0
        start = time.time()
        while True:
            try:
                if len(remove_list) > 1:
                    sql = f"DELETE FROM {table} WHERE uuid in {tuple(remove_list)};"
                else:
                    sql = f"DELETE FROM {table} WHERE uuid = '{list(remove_list)[0]}';"
                self.db.sql_cursor.execute(sql)
                self.db.conn.commit()
                return
            except sqlite3.OperationalError as e:
                return
            except Exception as e:  # pragma: no cover
                logger.error(f"{type(e)} Error in [remove_uuids]: {e}")
                return
            n += 1

    def denullify_stats_swaps(self):
        for column in [
            "maker_coin_usd_price",
            "taker_coin_usd_price",
            "maker_pubkey",
            "taker_pubkey",
        ]:
            if column in ["maker_coin_usd_price", "taker_coin_usd_price"]:
                value = 0
            if column in ["maker_pubkey", "taker_pubkey"]:
                value = "''"
            self.denullify_table("stats_swaps", column, value)

    @timed
    def denullify_table(self, table, column, value="''"):
        n = 0
        while True:
            try:
                sql = f"UPDATE {table} SET {column} = {value} WHERE {column} IS NULL"
                with self.db as db:
                    db.sql_cursor.execute(sql)
                    db.conn.commit()
                return default_result(
                    f"Nullification of {self.db.db_file} complete!",
                    loglevel='updated'
                )
            except sqlite3.OperationalError as e:
                msg = f"Error in [denullify_table] for {self.db_path}: {e}"
                return default_error(e, msg)
            except Exception as e:
                msg = f"Error in [denullify_table] for {self.db_path}: {e}"
                return default_error(e, msg)
