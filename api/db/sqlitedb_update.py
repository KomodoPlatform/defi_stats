#!/usr/bin/env python3
import time
import sqlite3
from util.files import Files
from util.utils import Utils
from const import templates
from util.logger import logger, timed
from util.templates import default_error, default_result
from util.enums import TablesEnum, ColumnsEnum



class SqliteUpdate:
    def __init__(self, db, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing"]
            templates.set_params(self, self.kwargs, self.options)
            self.utils = Utils(testing=self.testing)
            self.files = Files(testing=self.testing)
            self.db = db
        except Exception as e:
            error = f"{type(e)}: Failed to init SqliteQuery: {e}"
            return

    @timed
    def merge_db_tables(self, src_db, table, column, since=None):
        if since is None:
            since = int(time.time()) - 86400 * 7
        sql = ""
        try:
            src_columns = src_db.query.get_table_columns(table)
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
            self.db.sql_cursor.executescript(sql)
            msg = f"Importing {src_db.db_path} ==> {self.db.db_path} complete"
            return default_result(msg=msg, loglevel="updated")
        except sqlite3.OperationalError as e:
            msg = f"OpErr {src_db.db_path} ==> {self.db.db_path}"
            return default_error(e, msg=msg)
        except Exception as e:
            msg = f"{type(e)} {src_db.db_path} ==> {self.db.db_path} {e}"
            return default_error(e, msg=msg)


    @timed
    def remove_overlaps(self, remove_db):
        try:
            uuids = self.db.query.get_uuids()
            uuids_remove = remove_db.query.get_uuids(success_only=False)
            overlap = set(uuids_remove).intersection(set(uuids))
            if len(overlap) > 0:
                remove_db.remove_uuids(overlap)
                msg = f"{len(overlap)} uuids removed from {remove_db.db_path}"
            else:
                msg = f"No UUIDs to remove from {remove_db.db_path}"
            return default_result(msg=msg, loglevel='updated')
        except Exception as e:
            msg = f"{type(e)} Failed to remove UUIDs from {remove_db.db_path}: {e}"
            return default_error(e, msg=msg)
            

    @timed
    def update_stats_swap_row(self, uuid, data):
        n = 0
        while True:
            try:
                colvals = ",".join([f"{k} = {v}" for k, v in data.items()])
                t = (colvals, uuid)
                sql = f"UPDATE 'stats_swaps' SET ? WHERE uuid = ?;"
                self.db.sql_cursor.execute(sql, t)
                self.db.conn.commit()
                
                return default_result(msg=f"{uuid} updated in {db.db_file}")
            except sqlite3.OperationalError as e:
                return default_error(e)
            except Exception as e:
                return default_error(e)

    @timed
    def clear(self, table):
        try:
            self.db.sql_cursor.execute(f"DELETE FROM {table};")
            self.db.conn.commit()
            return
        except sqlite3.OperationalError as e:
            return
        except Exception as e:  # pragma: no cover
            return

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
            return default_result(msg, loglevel='muted')
        except sqlite3.OperationalError as e:
            return default_error(e)
        except Exception as e:  # pragma: no cover
            return default_error(e)

    @timed
    def remove_uuids(self, remove_list: set(), table: str = "stats_swaps") -> None:
        n = 0
        start = time.time()
        while True:
            try:
                if len(remove_list) > 1:
                    uuid_list = tuple(remove_list)
                else:
                    uuid_list = list(remove_list)[0]
                sql = f"DELETE FROM {TablesEnum[table]} WHERE uuid in ?;" 
                t = (table, uuid_list)
                self.db.sql_cursor.execute(sql, t)
                self.db.conn.commit()
                return
            except sqlite3.OperationalError as e:
                return
            except Exception as e:  # pragma: no cover
                logger.error(f"{type(e)} Error in [remove_uuids]: {e}")
                return
            n += 1

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
            except sqlite3.OperationalError as e:
                msg = f"{type(e)} for {self.db.db_path}: {e}"
                return default_error(e, msg)
            except Exception as e:
                msg = f"{type(e)} for {self.db.db_path}: {e}"
                return default_error(e, msg)
        return default_result(
            f"Nullification of {len(columns)} columns in {self.db.db_file} complete!",
            loglevel='updated'
        )


    @timed
    def denullify_table_column(self, table, column, value="''"):
        try:
            t = (value,)
            sql = f"UPDATE {TablesEnum[table]} SET {ColumnsEnum[column]} = ? WHERE {ColumnsEnum[column]} IS NULL"
            self.db.sql_cursor.execute(sql, t,)
            self.db.conn.commit()
            return default_result(
                f"Nullification of {column} in {self.db.db_file} complete!",
                loglevel='updated', ignore_until=10
            )
        except sqlite3.OperationalError as e:
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default_error(e, msg)
        except Exception as e:
            msg = f"{type(e)} for {self.db.db_path}: {e}"
            return default_error(e, msg)
