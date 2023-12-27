#!/usr/bin/env python3
import os
from os.path import basename
import time
import sqlite3
from const import (
    templates,
)
from db.sqlitedb_query import SqliteQuery
from db.sqlitedb_update import SqliteUpdate
from util.helper import get_sqlite_db_paths, get_netid
from util.logger import logger, timed
from util.templates import default_error, default_result
 

class SqliteDB:
    def __init__(self, db_path, **kwargs):
        try:
            self.kwargs = kwargs
            self.db_path = db_path
            self.db_file = basename(self.db_path)
            self.netid = get_netid(self.db_file)
            self.start = int(time.time())
            self.query = SqliteQuery(db=self)
            self.update = SqliteUpdate(db=self)
            self.options = ["testing", "wal", "dict_format"]
            templates.set_params(self, self.kwargs, self.options)
        except Exception as e:
            logger.error(f"{type(e)}: Failed to init SqliteDB: {e}")

    def __enter__(self):
        self.conn = self.connect()
        if self.dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        if self.wal:
            sql = "PRAGMA journal_mode=WAL;"
            self.sql_cursor.execute(sql)
            self.sql_cursor.fetchall()
        # logger.info(f"connected to {self.db_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @timed
    def close(self):
        self.conn.close()
        msg = f"Connection to {self.db_file} closed"
        return default_result(msg, loglevel='debug', ignore_until=10)
    
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


def view_locks(cursor):
    sql = "PRAGMA lock_status"
    r = cursor.execute(sql)
    return r.fetchall()

        
    return default_result("Database initialisation complete...", 'info')
