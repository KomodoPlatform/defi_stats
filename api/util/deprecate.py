

def dict_to_dict_list(data, key):
    dict_list = []
    for i in data:
        item = data[i]
        item.update({key: i})
        dict_list.append(item)
    return dict_list



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
    get_stopwatch(
        start,
        imported=True,
        context="SqliteDB.update_master_sqlite_dbs | Backup DB merge complete",
    )

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

