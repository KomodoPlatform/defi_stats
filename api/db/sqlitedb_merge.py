#!/usr/bin/env python3
import sys
import time
import inspect
from os import listdir
from os.path import dirname, abspath
API_ROOT_PATH = dirname(dirname(abspath(__file__)))
sys.path.append(API_ROOT_PATH)
from db.sqlitedb import list_sqlite_dbs, get_sqlite_db, get_mismatched_uuids, repair_swaps
from db.sqlitedb_update import SqliteUpdate
from db.sqlitedb_query import SqliteQuery
from const import DB_SOURCE_PATH, DB_CLEAN_PATH, MM2_DB_PATHS
from util.helper import is_source_db, get_netid
from util.logger import logger, get_trace, StopWatch



# This should be run on a separate server
# so processing is decoupled from serving.


# Step 1 - Merge source/ into cleaned/
# Step 2 - Denullify dbs in cleaned/
# Step 3 - Compare/repair in cleaned/
# Step 4 - Merge cleaned into master


def import_source_databases():
    for fn in list_sqlite_dbs(DB_SOURCE_PATH):
        if is_source_db(fn):
            src_db_path = f"{DB_SOURCE_PATH}/{fn}"
            src_db = get_sqlite_db(db_path=src_db_path)
            update = SqliteUpdate(db=src_db)
            update.denullify_stats_swaps()

            dest_db_path = f"{DB_CLEAN_PATH}/{fn}"
            dest_db = get_sqlite_db(db_path=dest_db_path)
            update = SqliteUpdate(db=dest_db)
            update.create_swap_stats_table()
            update.merge_db_tables(
                src_db=src_db,
                table="stats_swaps",
                column="uuid", since=0
            )
            src_db.close()
            dest_db.close()
        
    for fna in list_sqlite_dbs(DB_CLEAN_PATH):
        for fnb in list_sqlite_dbs(DB_CLEAN_PATH):
            if fna != fnb:
                context = f"Comparing {fna} to {fnb}"
                db1 = get_sqlite_db(db_path=fna)
                db2 = get_sqlite_db(db_path=fnb)
                uuids = get_mismatched_uuids(db1, db2)
                repair_swaps(uuids, db1, db2)


                for db in [db1, db2]:
                    netid = get_netid(db.db_file)
                    update = SqliteUpdate(db=db)
                if netid == "7777":
                    src_db = get_sqlite_db(db_path=MM2_DB_PATHS["temp_7777"])
                elif netid == "8762":
                    src_db = get_sqlite_db(db_path=MM2_DB_PATHS["temp_8762"])

                update.merge_db_tables(
                    src_db=src_db,
                    table="stats_swaps",
                    column="uuid", since=0
                )
                src_db.close()

                src_db_all = get_sqlite_db(db_path=MM2_DB_PATHS["temp_ALL"])
                update.merge_db_tables(
                    src_db=src_db_all,
                    table="stats_swaps",
                    column="uuid", since=0
                )
                src_db_all.close()
                

    temp_db_7777 = get_sqlite_db(db_path=MM2_DB_PATHS["temp_7777"])
    temp_db_8762 = get_sqlite_db(db_path=MM2_DB_PATHS["temp_8762"])
    temp_db_all = get_sqlite_db(db_path=MM2_DB_PATHS["temp_ALL"])
    temp_db_7777_query = SqliteQuery(db=temp_db_7777)
    temp_db_8762_query = SqliteQuery(db=temp_db_8762)
    temp_db_all_query = SqliteQuery(db=temp_db_all)

    temp_db_8762_update = SqliteUpdate(db=temp_db_8762)
    temp_db_8762_update.remove_overlaps(temp_db_7777)

    rows = temp_db_7777_query.get_row_count('stats_swaps')
    context = f"Temp DB 7777 has {rows} rows"
    rows = temp_db_8762_query.get_row_count('stats_swaps')
    context = f"Temp DB 8762 has {rows} rows"
    rows = temp_db_all_query.get_row_count('stats_swaps')
    context = f"Temp DB ALL has {rows} rows"
    
    
            
        

if __name__ == "__main__":
    import_source_databases()
