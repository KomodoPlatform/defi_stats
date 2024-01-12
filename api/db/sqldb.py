#!/usr/bin/env python3
import os
import json
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
import psycopg2
from schema import meta, stats_swaps
from dotenv import load_dotenv
from util.logger import logger

load_dotenv()


class SqlDB:
    def __init__(self, db_type, db_path=None) -> None:
        self.db_type = db_type
        self.db_path = db_path
        self.meta = meta
        if self.db_type == "pgsql":
            self.host = os.getenv("POSTGRES_HOST")
            self.user = os.getenv("POSTGRES_USER")
            self.password = os.getenv("POSTGRES_PASSWORD")
            self.port = os.getenv("POSTGRES_PORT")
            self.db_url = (
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        elif self.db_type == "sqlite":
            if self.db_path is not None:
                self.db_path = db_path
                self.db_url = f"sqlite:///{self.db_path}"

        elif self.db_type == "mysql":
            self.host = (os.getenv("ext_hostname"),)
            self.user = (os.getenv("ext_username"),)
            self.password = (os.getenv("ext_password"),)
            self.port = (os.getenv("ext_port"),)
            self.database = os.getenv("ext_db")
            self.db_url = (
                f"postgres://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        self.engine = create_engine(self.db_url, echo=True)
        self.meta.create_all(self.engine)


class SqlUpdate(SqlDB):
    def __init__(self, db_type, db_path=None) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path)


if __name__ == "__main__":
    pgsql = SqlUpdate("pgsql")
