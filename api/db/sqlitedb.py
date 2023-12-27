#!/usr/bin/env python3
import os
from os.path import basename
import time
from typing import List
import sqlite3
import inspect
from const import (
    PROJECT_ROOT_PATH,
    LOCAL_MM2_DB_PATH_7777,
    LOCAL_MM2_DB_PATH_8762,
    LOCAL_MM2_DB_BACKUP_7777,
    LOCAL_MM2_DB_BACKUP_8762,
    MM2_DB_PATHS,
    DB_SOURCE_PATH,
    templates,
)
from db.sqlitedb_query import SqliteQuery
from db.sqlitedb_update import SqliteUpdate
from util.helper import get_sqlite_db_paths, get_netid, is_source_db, is_7777
from util.logger import logger, get_trace, StopWatch
from util.enums import NetId
 

class SqliteDB:
    def __init__(self, db_path, **kwargs):
        try:
            self.kwargs = kwargs
            self.db_path = db_path
            self.db_file = basename(self.db_path)
            self.netid = get_netid(self.db_file)
            self.start = int(time.time())
            self.options = ["testing", "wal", "dict_format"]
            templates.set_params(self, self.kwargs, self.options)
        except Exception as e:
            logger.error(f"Failed to init {self.db_path}")

    def __enter__(self):
        self.conn = self.connect()
        if self.dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        if self.wal:
            sql = "PRAGMA journal_mode=WAL;"
            self.sql_cursor.execute(sql)
            self.sql_cursor.fetchall()
        logger.info(f"connected to {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        self.conn.close()
        runtime = self.start - int(time.time())
        logger.calc("exit db context")
        if runtime > 10:
            msg = f"Connection to {self.db_file} close after {runtime} sec"
            logger.debug(msg)
    
    def connect(self):
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


def backup_db(src_db_path: str, dest_db_path:str) -> None:
    try:
        src = get_sqlite_db(db_path=src_db_path)
        dest = get_sqlite_db(db_path=dest_db_path)
        src.conn.backup(dest.conn, pages=1, progress=progress)
        dest.close()
        src.close()
    except Exception as e:
        pass

def backup_local_dbs():
    # Backup the local active mm2 instance DBs
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
        pass


def get_mismatched_uuids(db1: SqliteDB, db2: SqliteDB):
    try:
        # Remove from 8762 if in 7777
        update_a = SqliteUpdate(db=db1)
        update_b = SqliteUpdate(db=db2)
        query_a = SqliteQuery(db=db1)
        query_b = SqliteQuery(db=db2)
        uuids_a = query_a.get_uuids(success_only=True)
        uuids_b = query_b.get_uuids(fail_only=True)
        mismatch_uuids = list(set(uuids_a).intersection(set(uuids_b)))
        context = f"{len(mismatch_uuids)} Mismatched UUIDS returned"
        return list(set(mismatch_uuids))
    except Exception as e:
        pass


def view_locks(cursor):
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()


def repair_swaps(uuids: List, db1: SqliteDB, db2: SqliteDB) -> None:
    try:
        db_list = [db1, db2]
        if len(uuids) == 0:
            print("UUid list is empty, no Swaps to repair!")
            return
        else:
            print(f"{len(uuids)} UUids to repair!")
            uuids.sort()
            repaired = 0
            for uuid in list(uuids):
                swap_infos = []
                try:
                    for db in db_list:
                        query = SqliteQuery(db=db)
                        swap_info = query.get_swap(uuid)
                        if "error" in swap_info:
                            continue
                        swap_infos.append(swap_info)

                    # logger.debug(f"Repairing swap {uuid}")
                    fixed = {}
                    for i in swap_infos:
                        for j in swap_infos:
                            for k, v in i.items():
                                if k not in['id']:
                                    for k2, v2 in j.items():
                                        if k == k2 and v != v2:
                                            '''
                                            logger.debug(
                                                f"Mismatch for {k}: {v} vs {k2}: {v2}"
                                            )
                                            '''
                                            # use higher value for below fields
                                            if k in [
                                                "is_success",
                                                "started_at",
                                                "finished_at",
                                                "maker_coin_usd_price",
                                                "taker_coin_usd_price",
                                            ]:
                                                try:
                                                    fixed.update({k: str(max([float(v), float(v2)]))})
                                                except Exception as e:
                                                    print(f"{v} vs {v2} | {type(v)} vs {type(v2)}")
                                            else:
                                                logger.warning(f"Unhandled mismatch on {k} for {uuid}")

                    if len(fixed) > 0:
                        for db in db_list:
                            update = SqliteUpdate(db=db)
                            update.update_stats_swap_row(uuid, fixed)
                except Exception as e:
                    pass
    except Exception as e:
        return


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
    logger.debug("Database initialisation complete...")


def init_stats_swaps_db(db):
    update = SqliteUpdate(db=db)
    update.create_swap_stats_table()


def setup_temp_dbs():
    for netid in NetId:
        db_path = MM2_DB_PATHS[f"temp_{netid.value}"]
        db = get_sqlite_db(db_path=db_path)
        query = SqliteQuery(db=db)
        update = SqliteUpdate(db=db)
        update.create_swap_stats_table()
        update.clear("stats_swaps")
        db.close()

init_dbs()
