#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.engine.url import URL
import mysql.connector
from util.defaults import default_result, set_params, default_error
from dotenv import load_dotenv

load_dotenv()


class MysqlDB:
    def __init__(self) -> None:
        self.engine = create_engine(self.db_url, echo=True)
        self.conn = self.connect()
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.close()

    def connect(self):
        try:
            return mysql.connector.connect(
                host=os.getenv("ext_hostname"),
                user=os.getenv("ext_username"),
                passwd=os.getenv("ext_password"),
                database=os.getenv("ext_db"),
            )
        except Exception as e:
            print(f"The error '{e}' occurred")

    def close(self):
        self.conn.close()
        msg = f"Connection to MySQL closed"
        return default_result(msg=msg, loglevel="debug", ignore_until=10)
