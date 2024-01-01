#!/usr/bin/env python3
import sys
import time
import sqlite3
from typing import List
from os.path import dirname, abspath
from const import (
    LOCAL_MM2_DB_PATH_7777,
    LOCAL_MM2_DB_PATH_8762,
    LOCAL_MM2_DB_BACKUP_7777,
    LOCAL_MM2_DB_BACKUP_8762,
    MM2_DB_PATHS,
    DB_SOURCE_PATH,
    DB_CLEAN_PATH,
)
from db.sqlitedb import (
    get_sqlite_db,
    is_source_db,
    get_netid,
    list_sqlite_dbs,
    SqliteDB,
)
from util.defaults import default_error, default_result
from util.enums import NetId
from util.logger import logger, timed


API_ROOT_PATH = dirname(dirname(abspath(__file__)))
sys.path.append(API_ROOT_PATH)


# This should be run on a separate server
# so processing is decoupled from serving.


# Step 1 - Merge source/ into cleaned/
# Step 2 - Denullify dbs in cleaned/
# Step 3 - Compare/repair in cleaned/
# Step 4 - Merge cleaned into master


@timed
def import_source_databases():
    backup_local_dbs()
    clean_source_dbs()
    compare_dbs()
    update_temp_dbs()
    get_db_row_counts(temp=True)
    update_master_dbs()
    get_db_row_counts()
    msg = "Souce database import completed!"
    return default_result(msg, loglevel="merge", ignore_until=10)


def clean_source_dbs():
    # Denullify source databases, move to 'clean' folder
    try:
        source_dbs = list_sqlite_dbs(DB_SOURCE_PATH)
        for fn in source_dbs:
            if is_source_db(fn):
                src_db_path = f"{DB_SOURCE_PATH}/{fn}"
                src_db = get_sqlite_db(db_path=src_db_path)
                src_db.update.denullify_stats_swaps()

                dest_db_path = f"{DB_CLEAN_PATH}/{fn}"
                dest_db = get_sqlite_db(db_path=dest_db_path)
                dest_db.update.create_swap_stats_table()
                merge_db_tables(
                    src_db=src_db,
                    dest_db=dest_db,
                    table="stats_swaps",
                    column="uuid",
                    since=0,
                )

                for i in [src_db, dest_db]:
                    i.close()
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = f"{len(source_dbs)} source databases cleaned."
    return default_result(msg, loglevel="merge")


@timed
def get_db_row_counts(temp=False):
    path = MM2_DB_PATHS["temp_7777"] if temp else MM2_DB_PATHS["7777"]
    db_7777 = get_sqlite_db(db_path=path)
    path = MM2_DB_PATHS["temp_8762"] if temp else MM2_DB_PATHS["8762"]
    db_8762 = get_sqlite_db(db_path=path)
    path = MM2_DB_PATHS["temp_ALL"] if temp else MM2_DB_PATHS["ALL"]
    db_all = get_sqlite_db(db_path=path)

    db_8762.update.remove_overlaps(db_7777)
    rows = db_7777.query.get_row_count("stats_swaps")
    msg_7777 = f"7777: {rows}"
    rows = db_8762.query.get_row_count("stats_swaps")
    msg_8762 = f"8762: {rows}"
    rows = db_all.query.get_row_count("stats_swaps")
    msg_ALL = f"ALL: {rows}"
    msg = f"Master DB rows: [{msg_7777}] [{msg_8762}] [{msg_ALL}]"

    for i in [db_all, db_8762, db_7777]:
        i.close()
    if temp:
        msg = f"Temp DB rows: [{msg_7777}] [{msg_8762}] [{msg_ALL}]"
    return default_result(msg, loglevel="merge")


@timed
def update_master_dbs():
    # Merge temp databases into master
    try:
        for i in NetId:
            i = i.value
            src_db = get_sqlite_db(db_path=MM2_DB_PATHS[f"temp_{i}"])
            dest_db = get_sqlite_db(db_path=MM2_DB_PATHS[f"{i}"])
            merge_db_tables(
                src_db=src_db,
                dest_db=dest_db,
                table="stats_swaps",
                column="uuid",
                since=0,
            )
            for i in [src_db, dest_db]:
                i.close()
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = "Merge of source data into master databases complete!"
    return default_result(msg=msg, loglevel="merge")


@timed
def update_temp_dbs():
    # Merge clean source databases into temp
    try:
        source_dbs = list_sqlite_dbs(DB_CLEAN_PATH)
        for fn in source_dbs:
            if not fn.startswith("temp"):
                if is_source_db(fn):
                    src_db_path = f"{DB_CLEAN_PATH}/{fn}"
                    src_db = get_sqlite_db(db_path=src_db_path)
                    netid = get_netid(fn)
                    dest_db_path = f"{DB_CLEAN_PATH}/temp_MM2_{netid}.db"
                    dest_db = get_sqlite_db(db_path=dest_db_path)
                    merge_db_tables(
                        src_db=src_db,
                        dest_db=dest_db,
                        table="stats_swaps",
                        column="uuid",
                        since=0,
                    )
                    for i in [src_db, dest_db]:
                        i.close()

        # Merge both netids into 'all'
        dest_db = get_sqlite_db(db_path=f"{DB_CLEAN_PATH}/temp_MM2_ALL.db")
        for i in NetId:
            i = i.value
            if i != "ALL":
                src_db = get_sqlite_db(db_path=f"{DB_CLEAN_PATH}/temp_MM2_{i}.db")
                merge_db_tables(
                    src_db=src_db,
                    dest_db=dest_db,
                    table="stats_swaps",
                    column="uuid",
                    since=0,
                )
                src_db.close()
            dest_db.close()
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = "Merge of source data into temp master databases complete!"
    return default_result(msg=msg, loglevel="merge")


@timed
def merge_db_tables(src_db, dest_db, table, column, since=None):
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
        dest_db.sql_cursor.executescript(sql)
    except sqlite3.OperationalError as e:
        msg = f"OpErr {src_db.db_path} ==> {dest_db.db_path}"
        return default_error(e, msg=msg)
    except Exception as e:
        msg = f"{type(e)} {src_db.db_path} ==> {dest_db.db_path} {e}"
        return default_error(e, msg=msg)
    msg = f"Importing {src_db.db_path} ==> {dest_db.db_path} complete"
    return default_result(msg=msg, loglevel="updated", ignore_until=10)


@timed
def compare_dbs():
    # Compare clean DBs to repair mismatches
    try:
        comparisons = 0
        clean_dbs = list_sqlite_dbs(DB_CLEAN_PATH)
        for fna in clean_dbs:
            for fnb in clean_dbs:
                if fna != fnb:
                    db1 = get_sqlite_db(db_path=f"{DB_CLEAN_PATH}/{fna}")
                    db2 = get_sqlite_db(db_path=f"{DB_CLEAN_PATH}/{fnb}")
                    uuids = get_mismatched_uuids(db1, db2)
                    repair_swaps(uuids, db1, db2)
                    comparisons += 1
                    db1.close()
                    db2.close()
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = f"Comparison of {len(clean_dbs)} databases in {comparisons} combinations complete!"
    return default_result(msg=msg, loglevel="merge", ignore_until=10)


@timed
def backup_local_dbs():
    # Backup the local active mm2 instance DBs
    try:
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_7777, dest_db_path=LOCAL_MM2_DB_BACKUP_7777
        )
        backup_db(
            src_db_path=LOCAL_MM2_DB_PATH_8762, dest_db_path=LOCAL_MM2_DB_BACKUP_8762
        )
    except Exception as e:
        return default_error(e)
    msg = "Merge of local source data into backup databases complete!"
    return default_result(msg, loglevel="merge")


# @timed
def get_mismatched_uuids(db1: SqliteDB, db2: SqliteDB):
    try:
        uuids_a = db1.query.get_uuids(success_only=True)
        uuids_b = db2.query.get_uuids(fail_only=True)
        mismatch_uuids = list(set(uuids_a).intersection(set(uuids_b)))
        return list(set(mismatch_uuids))
    except sqlite3.OperationalError as e:
        logger.error(e)
    except Exception as e:  # pragma: no cover
        logger.error(e)


@timed
def repair_swaps(uuids: List, db1: SqliteDB, db2: SqliteDB) -> None:
    try:
        db_list = [db1, db2]
        loglevel = "merged"
        if len(uuids) == 0:
            msg = "UUID list is empty, no swaps to repair!"
            loglevel = "muted"
        else:
            uuids.sort()
            for uuid in list(uuids):
                swap_infos = []
                for db in db_list:
                    swap_info = db.query.get_swap(uuid)
                    if "error" in swap_info:
                        continue
                    swap_infos.append(swap_info)
                compare_uuid_fields(uuid, swap_infos, db1, db2)

    except Exception as e:
        return default_error(e)
    msg = f"{len(uuids)} repaired in {db1.db_file},  {db2.db_file}"
    return default_result(msg, loglevel=loglevel)


def compare_uuid_fields(uuid: str, swap_infos: List, db1: SqliteDB, db2: SqliteDB):
    # logger.muted(f"Repairing swap {uuid}")
    try:
        fixed = {}
        for i in swap_infos:
            for j in swap_infos:
                for k, v in i.items():
                    if k not in ["id"]:
                        for k2, v2 in j.items():
                            if k == k2 and v != v2:
                                # use higher value for below fields
                                if k in [
                                    "is_success",
                                    "started_at",
                                    "finished_at",
                                    "maker_coin_usd_price",
                                    "taker_coin_usd_price",
                                ]:
                                    try:
                                        fixed.update(
                                            {
                                                k: str(
                                                    max(
                                                        [
                                                            float(v),
                                                            float(v2),
                                                        ]
                                                    )
                                                )
                                            }
                                        )
                                    except sqlite3.OperationalError as e:
                                        msg = f"{v} vs {v2} | {type(v)} vs {type(v2)}"
                                        return default_error(e, msg)
                            else:
                                msg = f"Unhandled mismatch on {k} for {uuid}"
                                return default_result(msg, loglevel="warning")
        db1.update.update_stats_swap_row(uuid, fixed)
    except Exception as e:
        return default_error(e)
    return default_result(msg=f"{uuid} repaired")


@timed
def init_dbs():
    try:
        for i in MM2_DB_PATHS:
            db = get_sqlite_db(db_path=MM2_DB_PATHS[i])
            init_stats_swaps_db(db)
            db.close()
        for i in [
            LOCAL_MM2_DB_BACKUP_7777,
            LOCAL_MM2_DB_PATH_7777,
            LOCAL_MM2_DB_BACKUP_8762,
            LOCAL_MM2_DB_PATH_8762,
        ]:
            db = get_sqlite_db(db_path=i)
            init_stats_swaps_db(db)
    except sqlite3.OperationalError as e:
        return default_error(e)
    except Exception as e:  # pragma: no cover
        return default_error(e)
    return default_result(
        "Database Initialisation complete!", loglevel="merge", ignore_until=10
    )


@timed
def setup_temp_dbs():
    try:
        for netid in NetId:
            db_path = MM2_DB_PATHS[f"temp_{netid.value}"]
            db = get_sqlite_db(db_path=db_path)
            db.update.create_swap_stats_table()
            db.update.clear("stats_swaps")
            db.close()
    except sqlite3.OperationalError as e:
        return default_error(e)
    except Exception as e:
        return default_error(e)
    msg = "Temp DBs setup complete..."
    return default_result(msg, "info")


@timed
def backup_db(src_db_path: str, dest_db_path: str) -> None:
    try:
        src = get_sqlite_db(db_path=src_db_path)
        dest = get_sqlite_db(db_path=dest_db_path)
        src.conn.backup(dest.conn, pages=1, progress=progress)
        src.close()
        dest.close()
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = f"Backup of {src.db_path} complete..."
    return default_result(msg, loglevel="muted")


def progress(status, remaining, total, show=False):
    if show:
        logger.muted(f"Copied {total-remaining} of {total} pages...")


@timed
def init_stats_swaps_db(db):
    try:
        db.update.create_swap_stats_table()
    except sqlite3.OperationalError as e:
        return default_error(e)
    except Exception as e:  # pragma: no cover
        return default_error(e)
    msg = f"Table 'stats_swaps' init for {db.db_file} complete..."
    return default_result(msg, loglevel="merge", ignore_until=10)


if __name__ == "__main__":
    init_dbs()
    import_source_databases()
