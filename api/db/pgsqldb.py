#!/usr/bin/env python3
import os
import json
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.engine.url import URL
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv

load_dotenv()


class SqlDB:
    def __init__(self, db_type, db_path=None) -> None:
        if db_type == "pgsql":
            self.host = os.getenv("POSTGRES_HOST")
            self.user = os.getenv("POSTGRES_USER")
            self.password = os.getenv("POSTGRES_PASSWORD")
            self.port = os.getenv("POSTGRES_PORT")
            self.db_url = (
                f"postgresql://{self.user}:{self.password}@127.0.0.1:{self.port}"
            )
        elif db_type == "sqlite":
            if db_path is not None:
                self.db_path = db_path
                self.db_url = f"sqlite://{self.db_path}"

        elif db_type == "mysql":
            self.host = (os.getenv("ext_hostname"),)
            self.user = (os.getenv("ext_username"),)
            self.password = (os.getenv("ext_password"),)
            self.port = (os.getenv("ext_port"),)
            self.database = os.getenv("ext_db")
            self.db_url = (
                f"postgres://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        self.engine = create_engine(self.db_url, echo=True)

    def __del__(self):
        self.engine.dispose()


class SqlUpdate(SqlDB):
    def __init__(self, db_type) -> None:
        super().__init__(db_type)
        self.init_stats_swap_table()

    def init_stats_swap_table(self):
        self.engine.execute(
            """
            CREATE TABLE IF NOT EXISTS
            stats_swaps (
                id INTEGER NOT NULL PRIMARY KEY,
                maker_coin VARCHAR(255) NOT NULL,
                taker_coin VARCHAR(255) NOT NULL,
                uuid VARCHAR(255) NOT NULL UNIQUE,
                started_at INTEGER NOT NULL,
                finished_at INTEGER NOT NULL,
                maker_amount DECIMAL NOT NULL,
                taker_amount DECIMAL NOT NULL,
                is_success INTEGER NOT NULL,
                maker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                maker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                taker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
                taker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
                maker_coin_usd_price DECIMAL NOT NULL DEFAULT 0,
                taker_coin_usd_price DECIMAL NOT NULL DEFAULT 0,
                maker_pubkey VARCHAR(255) NOT NULL DEFAULT '',
                taker_pubkey VARCHAR(255) NOT NULL DEFAULT ''
            );
            """
        )


if __name__ == "__main__":
    pgsql = SqlUpdate("pgsql")
