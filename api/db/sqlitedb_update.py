#!/usr/bin/env python3
import time
import sqlite3
import inspect
from os.path import basename
from random import randrange
from db.sqlitedb_query import SqliteQuery
from util.logger import logger
from util.files import Files
from util.utils import Utils
from const import templates
from util.helper import get_stopwatch, get_netid, get_trace

class SqliteUpdate:
    def __init__(self, db, **kwargs):
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

    def merge_db_tables(self, src_db, table, column, since=None):
        start = int(time.time())
        stack = inspect.stack()[1]
        context = get_trace(stack)
        if since is None:
            since = int(time.time()) - 86400 * 7
        sql = ""
        n = 0
        while True:
            try:
                src_db_query = SqliteQuery(db=src_db)
                src_columns = src_db_query.get_table_columns(table)
                src_columns.pop(src_columns.index("id"))
                sql = f"ATTACH DATABASE '{src_db.db_path}' AS src_db;"
                sql += f" INSERT INTO {table} ({','.join(src_columns)})"
                sql += f" SELECT {','.join(src_columns)}"
                sql += f" FROM src_db.{table}"
                sql += " WHERE NOT EXISTS ("
                sql += f"SELECT * FROM {table}"
                sql += f" WHERE {table}.{column} = src_db.{table}.{column})"
                sql += f" AND src_db.{table}.finished_at > {since};"
                sql += " DETACH DATABASE 'src_db';"
                self.sql_cursor.executescript(sql)
                context = f"Imported [{src_db.db_file}] into [{self.db_file}]"
                
                return get_stopwatch(start, imported=True, context=context)
            except sqlite3.OperationalError as e:
                if n > 10:
                    msg = f"Error in [merge_db_tables] for {src_db.db_file}"
                    msg += f" into {self.db_file}: {e}"
                    error = f"{type(e)}: {e}"
                    context = get_trace(stack, error)
                    return get_stopwatch(start, error=True, context=f"update.merge_db_tables {context}")
                msg = f"Error in [merge_db_tables] for {src_db.db_file}"
                msg += f" into {self.db_file}: {e}, retrying..."
                logger.warning(msg)
                time.sleep(randrange(20))
            except Exception as e:
                msg = f"Error in [merge_db_tables] for {src_db.db_file}"
                msg += f" into {self.db_file}: {e}, retrying..."
                logger.error(msg)
                logger.error(
                    {
                        "error": str(e),
                        "type": type(e),
                        "msg": msg,
                        "src_db": src_db.db_path,
                        "dest_db": self.db_path,
                        "sql": sql,
                    }
                )
            n += 1

    def remove_overlaps(self, remove_db):
        start = int(time.time())
        uuids = self.query.get_uuids()
        query_remove_db = SqliteQuery(db=remove_db)
        uuids_remove = query_remove_db.get_uuids(success_only=False)
        overlap = set(uuids_remove).intersection(set(uuids))
        if len(overlap) > 0:
            remove_db.remove_uuids(overlap)
            context = f"Removed {len(overlap)} rows"
            context += f"from {remove_db.db_file} where already in {self.db_file}"
            get_stopwatch(start, context=context)

    def update_stats_swap_row(self, uuid, data):
        n = 0
        while True:
            try:
                colvals = ",".join([f"{k} = {v}" for k, v in data.items()])
                sql = f"UPDATE {'stats_swaps'} SET {colvals} WHERE uuid = '{uuid}';"
                self.sql_cursor.execute(sql)
                self.conn.commit()
                logger.info(f"{uuid} repaired for netid {self.db_file}!")
                return
            except sqlite3.OperationalError as e:
                logger.error(f"Error in [update_stats_swap_row]: {e}")
                if n > 10:
                    return
                n += 1
                logger.warning(f"Error in [update_stats_swap_row]: {e}, retrying...")
                time.sleep(randrange(20))
            except Exception as e:
                logger.error(f"Error in [update_stats_swap_row]: {e}")
                return

    def clear(self, table):
        n = 0
        while True:
            try:
                self.sql_cursor.execute(f"DELETE FROM {table}")
                self.conn.commit()
                return
            except sqlite3.OperationalError as e:
                if n > 10:
                    logger.error(f"{type(e)} Error in [clear]: {e}")
                    return
                n += 1
                logger.warning(f"Error in [clear]: {e}, retrying...")
                time.sleep(randrange(20))
            except Exception as e:  # pragma: no cover
                logger.error(f"{type(e)} Error in [clear]: {e}")
                return

    def create_swap_stats_table(self):
        n = 0
        while True:
            try:
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
                return
            except sqlite3.OperationalError as e:
                if n > 10:
                    logger.error(f"{type(e)} Error in [create_swap_stats_table]: {e}")
                    return
                n += 1
                logger.warning(f"Error in [create_swap_stats_table]: {e}, retrying...")
                time.sleep(randrange(20))
            except Exception as e:  # pragma: no cover
                logger.error(f"{type(e)} Error in [create_swap_stats_table]: {e}")
                return

    def remove_uuids(self, remove_list: set(), table: str = "stats_swaps") -> None:
        n = 0
        start = time.time()
        while True:
            try:
                if len(remove_list) > 1:
                    sql = f"DELETE FROM {table} WHERE uuid in {tuple(remove_list)};"
                else:
                    sql = f"DELETE FROM {table} WHERE uuid = '{list(remove_list)[0]}';"
                self.sql_cursor.execute(sql)
                self.conn.commit()
                get_stopwatch(start, context=f"{table} for {self.db_path}")
                return
            except sqlite3.OperationalError as e:
                if n > 10:
                    logger.error(f"{type(e)} Error in [remove_uuids]: {e}")
                    return
                logger.warning(f"Error in [remove_uuids]: {e}, retrying...")
                time.sleep(randrange(20))
            except Exception as e:  # pragma: no cover
                logger.error(f"{type(e)} Error in [remove_uuids]: {e}")
                return
            n += 1

    def denullify_stats_swaps(self):
        start = int(time.time())
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
        get_stopwatch(start, context=f"{self.db_path} [{column}]")

    def denullify_table(self, table, column, value="''"):
        n = 0
        while True:
            try:
                sql = f"UPDATE {table} SET {column} = {value} WHERE {column} IS NULL"
                self.sql_cursor.execute(sql)
                self.conn.commit()
                return
            except sqlite3.OperationalError as e:
                if n > 10:
                    logger.error(f"Error in [denullify_table] for {self.db_file}: {e}")
                    return
                n += 1
                logger.warning(
                    f"Error in [denullify_table] for {self.db_file}: {e}, retrying..."
                )
                time.sleep(randrange(30))
            except Exception as e:
                logger.error(f"Error in [denullify_table] for {self.db_file}: {e}")
                return
