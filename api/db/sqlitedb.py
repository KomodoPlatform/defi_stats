#!/usr/bin/env python3
from os.path import basename
from util.cron import cron
import sqlite3
from typing import List
from const import MM2_DB_PATHS, MM2_NETID
from util.enums import TablesEnum, NetId, ColumnsEnum
from util.exceptions import InvalidParamCombination
from util.files import Files
from util.logger import logger, timed
import util.defaults as default
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
            self.db = db
        except Exception as e:  # pragma: no cover
            logger.error(f"{type(e)}: Failed to init SqliteQuery: {e}")

    def get_table_columns(self, table):
        sql = f"SELECT * FROM '{table}' LIMIT 1;"
        r = self.db.sql_cursor.execute(sql)
        r.fetchone()
        return [i[0] for i in r.description]

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
            return default.result(msg=e, loglevel="warning")

    @timed
    def get_row_count(self, table):
        try:
            self.db.sql_cursor.execute(f"SELECT COUNT(*) FROM {TablesEnum[table]}")
            r = self.db.sql_cursor.fetchone()
            return r[0]
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")

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
            return default.result(msg=e, loglevel="warning")
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")

    @timed
    def clear(self, table):
        try:
            self.db.sql_cursor.execute(f"DELETE FROM {table};")
            self.db.conn.commit()
            return
        except sqlite3.OperationalError as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")

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
            return default.result(msg=e, loglevel="warning")
        except Exception as e:  # pragma: no cover
            return default.result(msg=e, loglevel="warning")

    @timed
    def remove_uuids(self, remove_list, table: str = "stats_swaps") -> None:
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
