#!/usr/bin/env python3
from os import listdir
from util.logger import logger
from util.enums import NetId
from db.sqlitedb import SqliteDB
from const import DB_SOURCE_PATH, MM2_DB_PATHS

# This should be run on a separate server
# so processing is decoupled from serving.


def get_sqlite_db(db_path: str) -> SqliteDB:
    db = SqliteDB(db_path=db_path)
    return db


def is_7777(fn: str) -> bool:
    # netid 8762 starts with a dragon name and ends with `_MM2.db`
    if fn.startswith("seed"):
        return True
    return False


def is_source_db(fn: str) -> bool:
    # temp and master dbs are in the same folder,
    # but end in thier netid value
    if fn.endswith("MM2.db"):
        return True
    return False


def list_sqlite_dbs(folder):
    # Do not return other filetypes
    db_list = [i for i in listdir(folder) if i.endswith(".db")]
    db_list.sort()
    return db_list


def setup_temp_dbs():
    for netid in NetId.values():
        db_path = MM2_DB_PATHS[f"temp_{netid}"]
        db = get_sqlite_db(db_path=db_path)
        db.create_swap_stats_table()
        db.clear("stats_swaps")


def merge_dbs():
    for fn in list_sqlite_dbs(DB_SOURCE_PATH):
        if is_source_db(fn):
            src_db_path = f"{DB_SOURCE_PATH}/{fn}"
            if is_7777(fn):
                print(f"7777: {fn}")
                src_db = get_sqlite_db(db_path=src_db_path)
                src_db.denullify_stats_swaps()
                dest_db = get_sqlite_db(db_path=MM2_DB_PATHS["temp_7777"])
                dest_db.merge_db_tables(
                    src_db=src_db, table="stats_swaps", column="uuid", since=0
                )
            else:
                print(f"8762: {fn}")
                src_db = get_sqlite_db(db_path=src_db_path)
                src_db.denullify_stats_swaps()
                dest_db = get_sqlite_db(db_path=MM2_DB_PATHS["temp_8762"])
                dest_db.merge_db_tables(
                    src_db=src_db, table="stats_swaps", column="uuid", since=0
                )
            src_db.close()
            dest_db.close()
        else:
            print(f"Other: {fn}")

    # Now merge the two temp 'netid' dbs into the temp 'all' db
    dest_db = get_sqlite_db(db_path=MM2_DB_PATHS["temp_ALL"])
    for netid in [7777, 8762]:
        src_db = get_sqlite_db(db_path=MM2_DB_PATHS[f"temp_{netid}"])
        dest_db.merge_db_tables(
            src_db=src_db, table="stats_swaps", column="uuid", since=0
        )
    src_db.close()
    dest_db.close()


if __name__ == "__main__":
    setup_temp_dbs()
    merge_dbs()
