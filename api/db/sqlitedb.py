#!/usr/bin/env python3
import os
from os.path import basename
import time
import sqlite3
import inspect
from const import (
    PROJECT_ROOT_PATH,
    LOCAL_MM2_DB_PATH_7777,
    LOCAL_MM2_DB_PATH_8762,
    LOCAL_MM2_DB_BACKUP_7777,
    LOCAL_MM2_DB_BACKUP_8762,
    MM2_DB_PATHS,
    templates,
)
from db.sqlitedb_query import SqliteQuery
from db.sqlitedb_update import SqliteUpdate
from util.logger import logger
from util.helper import get_sqlite_db_paths, get_stopwatch, get_netid, get_trace


class SqliteDB:
    def __init__(self, db_path, **kwargs):
        self.kwargs = kwargs
        self.db_path = db_path
        self.db_file = basename(self.db_path)
        self.netid = get_netid(self.db_file)
        self.start = int(time.time())
        self.options = ["testing", "wal", "dict_format"]
        templates.set_params(self, self.kwargs, self.options)
        self.conn = self.connect()
        if self.dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        if self.wal:
            sql = "PRAGMA journal_mode=WAL;"
            self.sql_cursor.execute(sql)
            self.sql_cursor.fetchall()

    def close(self):
        self.conn.close()
        runtime = self.start - int(time.time())
        if runtime > 10:
            msg = f"Connection to {self.db_file} close after {runtime} sec"
            logger.debug(msg)

    def connect(self, wal=True):
        return sqlite3.connect(self.db_path)


def get_sqlite_db(
    db_path=None, testing: bool = False, DB=None, dict_format=False, netid=None
):
    if DB is not None:
        return DB

    if netid is not None:
        db_path = get_sqlite_db_paths(netid)
    db = SqliteDB(db_path=db_path, testing=testing, dict_format=dict_format)
    # logger.info(f"Connected to DB [{db.db_path}]")
    return db


def list_sqlite_dbs(folder):
    db_list = [i for i in os.listdir(folder) if i.endswith(".db")]
    db_list.sort()
    return db_list


def progress(status, remaining, total, show=False):
    if show:
        logger.debug(f"Copied {total-remaining} of {total} pages...")


def backup_db(src_db_path, dest_db_path):
    src = get_sqlite_db(db_path=src_db_path)
    dest = get_sqlite_db(db_path=dest_db_path)
    src.conn.backup(dest.conn, pages=1, progress=progress)
    # logger.debug(f'Backed up {src_db_path} to {dest_db_path}')
    dest.close()
    src.close()


def backup_local_dbs():
    # Backup the local active mm2 instance DBs
    start = int(time.time())
    stack = inspect.stack()[1]
    context = get_trace(stack)
    try:
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_7777, dest_db_path=LOCAL_MM2_DB_BACKUP_7777
        )
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_8762, dest_db_path=LOCAL_MM2_DB_BACKUP_8762
        )
        db = get_sqlite_db(db_path=LOCAL_MM2_DB_BACKUP_8762)
        update = SqliteUpdate(db=db)
        update.denullify_stats_swaps()
        db = get_sqlite_db(db_path=LOCAL_MM2_DB_BACKUP_7777)
        update = SqliteUpdate(db=db)
        update.denullify_stats_swaps()
        return {"result": "backed up local databases"}
    except Exception as e:
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        get_stopwatch(start, error=True, context=context)


def update_master_sqlite_dbs():
    start = int(time.time())
    stack = inspect.stack()[1]
    context = get_trace(stack)
    backup_local_dbs()

    # Get list of supplemental db files
    db_folder = f"{PROJECT_ROOT_PATH}/DB"
    sqlite_db_list = list_sqlite_dbs(db_folder)
    sqlite_db_list.sort()

    # Open master databases
    db_all = get_sqlite_db(db_path=MM2_DB_PATHS["ALL"])
    db_temp = get_sqlite_db(db_path=MM2_DB_PATHS["temp_ALL"])
    db_7777 = get_sqlite_db(db_path=MM2_DB_PATHS["7777"])
    db_8762 = get_sqlite_db(db_path=MM2_DB_PATHS["8762"])
    local_db_8762_backup = get_sqlite_db(db_path=LOCAL_MM2_DB_BACKUP_8762)
    local_db_7777_backup = get_sqlite_db(db_path=LOCAL_MM2_DB_BACKUP_7777)

    update_7777_db = SqliteUpdate(db=db_7777)
    update_8762_db = SqliteUpdate(db=db_8762)
    update_all_db = SqliteUpdate(db=db_all)
    update_temp_db = SqliteUpdate(db=db_temp)

    query_7777_db = SqliteQuery(db=db_7777)
    query_all_db = SqliteQuery(db=db_all)

    try:
        # Merge local into master databases. Defer import into 8762.
        update_7777_db.merge_db_tables(
            src_db=local_db_7777_backup, table="stats_swaps", column="uuid"
        )
        update_all_db.merge_db_tables(
            src_db=local_db_7777_backup, table="stats_swaps", column="uuid"
        )
        update_all_db.merge_db_tables(
            src_db=local_db_8762_backup, table="stats_swaps", column="uuid"
        )
    except Exception as e:
        logger.warning(f"Backup DB Merge failed: {e}")
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        return get_stopwatch(start, error=True, context=context)
    get_stopwatch(start, imported=True, context="Backup DB merge complete")
    
    try:
        # Handle 7777 first
        for source_db_file in sqlite_db_list:
            if not source_db_file.startswith("MM2"):
                source_db_path = f"{db_folder}/{source_db_file}"
                src_db = get_sqlite_db(db_path=source_db_path)
                if source_db_file.startswith("seed"):
                    # Import into 7777
                    update_7777_db.merge_db_tables(
                        src_db=src_db, table="stats_swaps", column="uuid"
                    )
                # Import into ALL
                update_all_db.merge_db_tables(
                    src_db=src_db, table="stats_swaps", column="uuid"
                )
    except Exception as e:
        logger.warning(f"Source DB Merge failed: {e}")

    inspect_data(db_7777, db_8762, db_all)
    # import all into temp
    update_temp_db.merge_db_tables(src_db=db_all, table="stats_swaps", column="uuid")
    uuids_7777 = query_7777_db.get_uuids()
    uuids_temp = query_all_db.get_uuids()
    overlap = set(uuids_temp).intersection(set(uuids_7777))
    if len(overlap) > 0:
        update_temp_db.remove_uuids(overlap)

    # Import from temp into 8762 after 7777 removed

    update_8762_db.merge_db_tables(src_db=db_temp, table="stats_swaps", column="uuid")

    # Close master databases
    db_7777.close()
    db_8762.close()
    db_all.close()
    # Clear the temp database
    update_temp_db.clear("stats_swaps")
    db_temp.close()
    return {"result": "merge to master databases complete"}


def inspect_data(db_7777, db_8762, db_all):
    start = int(time.time())
    stack = inspect.stack()[1]
    context = get_trace(stack)
    try:
        # Remove from 8762 if in 7777
        update_7777_db = SqliteUpdate(db=db_7777)
        update_all_db = SqliteUpdate(db=db_all)

        query_7777_db = SqliteQuery(db=db_7777)
        query_8762_db = SqliteQuery(db=db_8762)
        query_all_db = SqliteQuery(db=db_all)

        update_7777_db.remove_overlaps(db_8762)

        uuids_7777 = query_7777_db.get_uuids()
        uuids_8762 = query_8762_db.get_uuids()
        uuids_all = query_all_db.get_uuids()

        extras = list(set(uuids_all) - set(uuids_7777) - set(uuids_8762))
        context = f"{len(extras)} uuids in ALL but not in 8762 or 7777"
        extras2 = list(set(list(uuids_7777) + list(uuids_8762)) - set(uuids_all))
        context = f"{len(extras2)} uuids in 8762 or 7777 but not in ALL"
        inspect = list(set(extras + extras2))
        context = f"{len(inspect)} records with missing info to inspect..."
    except Exception as e:
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        get_stopwatch(start, error=True, context=context)
    try:
        if len(inspect) > 0:
            inspect.sort()
        for i in list(inspect):
            try:
                swap_8762 = query_8762_db.get_swap(i)
                if "error" in swap_8762:
                    continue

                swap_7777 = query_7777_db.get_swap(i)
                if "error" in swap_7777:
                    continue

                swap_all = query_all_db.get_swap(i)
                if "error" in swap_all:
                    continue

                logger.debug(f"Repairing swap {i}")
                fixed = {}
                for k, v in swap_7777.items():
                    if k != "id":
                        if k in swap_8762:
                            if swap_8762[k] != v:
                                logger.debug(
                                    f"UUID [{i}] duplicate mismatch for {k}: {v} \
                                        (7777) vs {swap_8762[k]} (8762)"
                                )
                                if k in [
                                    "is_success",
                                    "started_at",
                                    "finished_at",
                                    "maker_coin_usd_price",
                                    "taker_coin_usd_price",
                                ]:
                                    fixed.update({k: max([v, swap_8762[k]])})

                        if k in swap_all:
                            if swap_all[k] != v:
                                logger.debug(
                                    f"UUID [{i}] duplicate mismatch for {k}: {v} \
                                        (7777) vs {swap_all[k]} (all)"
                                )
                                if k in [
                                    "is_success",
                                    "started_at",
                                    "finished_at",
                                    "maker_coin_usd_price",
                                    "taker_coin_usd_price",
                                ]:
                                    fixed.update({k: max([v, swap_all[k]])})
                if len(fixed) > 0:
                    db_7777.update_stats_swap_row(i, fixed)
                    db_8762.update_stats_swap_row(i, fixed)
                    db_all.update_stats_swap_row(i, fixed)

            except Exception as e:
                logger.error(f"Failed to repair swap [{i}]: {e}")
                logger.debug(f"swap_7777: {swap_7777}")
                logger.debug(f"swap_8762: {swap_8762}")
                error = f"{type(e)}: {e}"
                context = get_trace(stack, error)
                get_stopwatch(start, error=True, context=context)
    except Exception as e:
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        get_stopwatch(start, error=True, context=context)
    get_stopwatch(start, context="repaired mismatched swaps")

    try:

        # In case not yet in ALL
        update_all_db.merge_db_tables(
            src_db=db_7777, table="stats_swaps", column="uuid"
        )
        update_all_db.merge_db_tables(
            src_db=db_8762, table="stats_swaps", column="uuid"
        )
        get_stopwatch(start, context="Importing complete")

        uuids_7777 = query_7777_db.get_uuids()
        uuids_8762 = query_8762_db.get_uuids()
        uuids_all = query_all_db.get_uuids()

        extras = list(set(uuids_all) - set(uuids_7777) - set(uuids_8762))
        context = f"{len(extras)} uuids in ALL but not in 8762 or 7777"
        get_stopwatch(start, context=context, debug=True)
        extras2 = list(set(list(uuids_7777) + list(uuids_8762)) - set(uuids_all))
        context = f"{len(extras2)} uuids in 8762 or 7777 but not in ALL"
        get_stopwatch(start, context=context, debug=True)
    except Exception as e:
        error = f"{type(e)}: {e}"
        context = get_trace(stack, error)
        get_stopwatch(start, error=True, context=context)


def view_locks(cursor):
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()


def init_dbs():
    logger.debug("Initialising databases...")
    for i in MM2_DB_PATHS:
        db = get_sqlite_db(db_path=MM2_DB_PATHS[i])
        init_stats_swaps_db(db)
    for i in [
        LOCAL_MM2_DB_BACKUP_7777,
        LOCAL_MM2_DB_PATH_7777,
        LOCAL_MM2_DB_BACKUP_8762,
        LOCAL_MM2_DB_PATH_8762,
    ]:
        db = get_sqlite_db(db_path=i)
        init_stats_swaps_db(db)


def init_stats_swaps_db(db):
    update = SqliteUpdate(db=db)
    update.create_swap_stats_table()
    db.close()


init_dbs()
