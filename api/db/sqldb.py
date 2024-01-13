#!/usr/bin/env python3
import os
import json
import psycopg2
from datetime import datetime
from typing import Optional
from sqlmodel import Field, Session, SQLModel, create_engine, select, text
from sqlalchemy import func

from db.schema import CipiSwaps
from dotenv import load_dotenv
from util.logger import logger
from const import (
    MYSQL_USERNAME,
    MYSQL_HOSTNAME,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
    POSTGRES_HOST,
    POSTGRES_USERNAME,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
)

load_dotenv()


class SqlDB:
    def __init__(self, db_type, db_path=None, external=False) -> None:
        self.db_type = db_type
        self.db_path = db_path
        self.external = external
        if self.db_type == "pgsql":
            self.host = POSTGRES_HOST
            self.user = POSTGRES_USERNAME
            self.password = POSTGRES_PASSWORD
            self.port = POSTGRES_PORT
            self.db_url = (
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        elif self.db_type == "sqlite":
            if self.db_path is not None:
                self.db_path = db_path
                self.db_url = f"sqlite:///{self.db_path}"

        elif self.db_type == "mysql":
            self.external = True
            self.host = MYSQL_HOSTNAME
            self.user = MYSQL_USERNAME
            self.password = MYSQL_PASSWORD
            self.port = MYSQL_PORT
            self.database = MYSQL_DATABASE
            self.db_url = f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        self.engine = create_engine(self.db_url, echo=True)
        with Session(self.engine) as session:
            if self.external:
                stmt = text("DESCRIBE swaps;")
                logger.loop(stmt)
                r = session.exec(stmt)
                logger.merge(r)
                for i in r:
                    logger.merge(i)

                stmt = text("select * from swaps limit 5;")
                logger.loop(stmt)
                r = session.exec(stmt)
                logger.merge(r)
                for i in r:
                    logger.merge(i)

                stmt = select(CipiSwaps.uuid).limit(4)
                logger.loop(stmt)
                r = session.exec(stmt)
                logger.merge(r)
                for i in r:
                    logger.merge(i)
            else:
                SQLModel.metadata.create_all(self.engine)
                logger.merge("Created PGSQL Table")
                

class SqlUpdate(SqlDB):
    def __init__(self, db_type, db_path=None, external=False) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)

class SqlQuery(SqlDB):
    def __init__(self, db_type, db_path=None, external=False) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)

    def get_distinct(self, table: object):
        with Session(self.engine) as session:
                pass

    def get_count(self, table: object):
        with Session(self.engine) as session:
            r = session.query(func.count(table))
            return r[0][0]

    def get_swaps(self, table: object, start: int, end: int):
        with Session(self.engine) as session:
            start = datetime.fromtimestamp(start)
            end = datetime.fromtimestamp(end)
            r = session.query(CipiSwaps).filter(CipiSwaps.started_at > start).filter(CipiSwaps.started_at < end).order_by(CipiSwaps.started_at).all()
            return [dict(i) for i in r]
        
if __name__ == "__main__":
    pgsql = SqlUpdate("pgsql")
