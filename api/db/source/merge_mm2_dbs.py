#!/usr/bin/env python3
from api.util.logger import logger
from api.db.sqlitedb import list_sqlite_dbs, get_sqlite_db, init_stats_swaps_db
from api.util.enums import NetId

MM2_DB_PATH = "/home/komodian/MM2_DB"


def setup_temp_dbs():
    for netid in NetId.values():
        db_path = f"{MM2_DB_PATH}/temp_MM2_{netid}.db"
        db = get_sqlite_db(db_path=db_path)
        init_stats_swaps_db(db)
        db.update.clear("stats_swaps")


def merge_dbs():
    for fn in list_sqlite_dbs(MM2_DB_PATH):
        if is_source_db(fn):
            db_path = f"{MM2_DB_PATH}/{fn}"
            if is_7777(fn):
                print(f"7777: {fn}")
                db = get_sqlite_db(db_path=db_path)
                init_stats_swaps_db(db)
            else:
                print(f"8762: {fn}")
                db = get_sqlite_db(db_path=db_path)
                init_stats_swaps_db(db)
        else:
            print(f"Other: {fn}")


if __name__ == "__main__":
    setup_temp_dbs()
    merge_dbs()
