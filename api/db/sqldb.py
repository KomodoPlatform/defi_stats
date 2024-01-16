#!/usr/bin/env python3
import time
from decimal import Decimal
from datetime import datetime, timezone
from sqlalchemy import func
from sqlalchemy.sql.expression import bindparam
from sqlmodel import Session, SQLModel, create_engine, text, update, select, or_
from db.schema import CipiSwap, DefiSwap, StatsSwap
from dotenv import load_dotenv
from util.defaults import default_error, default_result
from util.logger import logger, timed
import util.transform as transform
import lib
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
    MM2_DB_PATH_ALL,
)

load_dotenv()


class SqlDB:
    def __init__(self, db_type="pgsql", db_path=None, external=False, **kwargs) -> None:
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
            self.db_url = f"mysql://{self.user}:{self.password}@{self.host}:{self.port}"
            self.db_url += f"/{self.database}"

        self.engine = create_engine(self.db_url)  # ), echo=True)

        if "gecko_source" in kwargs:
            self.gecko_source = kwargs["gecko_source"]
        else:
            self.gecko_source = lib.cache.load_gecko_source()
        if "coins_config" in kwargs:
            self.coins_config = kwargs["coins_config"]
        else:
            self.coins_config = lib.cache.load_coins_config()


class SqlUpdate(SqlDB):
    def __init__(self, db_type="pgsql", db_path=None, external=False) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)

    def drop(self, table):
        try:
            with Session(self.engine) as session:
                session.exec(text(f"DROP TABLE {get_tablename(table)};"))
                session.commit()
        except Exception as e:
            logger.warning(e)


class SqlQuery(SqlDB):
    def __init__(self, db_type="pgsql", db_path=None, external=False) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)

    """
    def get_distinct(self, table: object):
        with Session(self.engine) as session:
            pass
    """

    def get_count(self, table: object):
        with Session(self.engine) as session:
            r = session.query(func.count(table))
            return r[0][0]

    def get_last(self, table: str, limit: int = 3):
        try:
            with Session(self.engine) as session:
                sql = f"SELECT * FROM {table} LIMIT {limit};"
                r = session.exec(text(sql))
                return [dict(i) for i in r]
        except Exception as e:
            logger.error(e)

    def get_first(self, table: str, limit: int = 3):
        try:
            with Session(self.engine) as session:
                sql = f"SELECT * FROM {table} ORDER BY started_at ASC LIMIT {limit};"
                r = session.exec(text(sql))
                return [dict(i) for i in r]
        except Exception as e:
            logger.error(e)

    def describe(self, table):
        with Session(self.engine) as session:
            stmt = text(f"DESCRIBE {get_tablename(table)};")
            logger.loop(stmt)
            r = session.exec(stmt)
            logger.merge(r)
            for i in r:
                logger.merge(i)

    @timed
    def get_swaps(
        self,
        table: object = DefiSwap,
        start_time: int = 0,
        end_time: int = 0,
        coin=None,
        pair=None,
    ):
        """
        Returns swaps matching filter from any of the SQL databases.
        For MM2 and Cipi's databases, some fields are derived or set
        to default as they are not present in the source. The Pgsql
        'defi_stats' database contains data imported from the above,
        using the higher value for any numeric fields, and with defaults
        reconciled (if available in either of the MM2/Cipi databases).

        For `pair` or `coin`, it will return all variants to be combined
        (or further filtered) later.
        """
        try:
            if start_time == 0:
                start_time = int(time.time()) - 86400
            if end_time == 0:
                end_time = int(time.time())
            if table.__tablename__ in ["swaps", "swaps_failed"]:
                start_time = datetime.fromtimestamp(start_time, timezone.utc)
                end_time = datetime.fromtimestamp(end_time, timezone.utc)
            with Session(self.engine) as session:
                q = select(table).where(
                    table.started_at > start_time, table.started_at < end_time
                )
                if coin is not None:
                    q = q.where(
                        or_(
                            coin == table.maker_coin_ticker,
                            coin == table.taker_coin_ticker,
                        )
                    )
                elif pair is not None:
                    pair = transform.strip_pair_platforms(pair)
                    pair = transform.order_pair_by_market_cap(
                        pair, gecko_source=self.gecko_source
                    )
                    base, quote = pair.split("_")
                    q = q.where(
                        or_(
                            base == table.maker_coin_ticker,
                            base == table.taker_coin_ticker,
                        )
                    ).where(
                        or_(
                            quote == table.maker_coin_ticker,
                            quote == table.taker_coin_ticker,
                        )
                    )

                q = q.order_by(table.started_at)
                r = session.exec(q)
                data = [dict(i) for i in r]

                if coin is not None:
                    variants = [i for i in self.coins_config if i.startswith(coin)]
                    for i in data:
                        logger.calc(i)
                        logger.info(i["taker_coin"])
                    resp = {
                        i: [j for j in data if i in [j["taker_coin"], j["maker_coin"]]]
                        for i in variants
                    }
                    resp.update({"ALL": data})
                elif pair is not None:
                    base_variants = [i for i in self.coins_config if i.startswith(base)]
                    quote_variants = [
                        i for i in self.coins_config if i.startswith(quote)
                    ]
                    resp = {}
                    for i in base_variants:
                        for j in quote_variants:
                            resp.update(
                                {
                                    f"{i}_{j}": [
                                        k
                                        for k in data
                                        if i == k["taker_coin"] and j == k["maker_coin"]
                                    ]
                                }
                            )
                    resp.update({"ALL": data})
                else:
                    resp = data
        except Exception as e:
            return default_error(e)
        msg = f"Got {len(data)} swaps from {table.__tablename__}"
        msg += f" between {start_time} and {end_time}"
        if coin is not None:
            msg += f" [{coin}]"
        if pair is not None:
            msg += f" [{pair}]"
        return default_result(data=resp, msg=msg, loglevel="muted")


@timed
def normalise_swap_data(data, gecko_source, is_success=None):
    try:
        for i in data:
            pair = f'{i["maker_coin"]}_{i["taker_coin"]}'
            pair_std = transform.order_pair_by_market_cap(
                pair, gecko_source=gecko_source
            )
            if pair.replace("-segwit", "") == pair_std:
                trade_type = "buy"
            else:
                trade_type = "sell"
            i.update(
                {
                    "pair": pair,
                    "trade_type": trade_type,
                    "maker_coin_ticker": transform.strip_coin_platform(i["maker_coin"]),
                    "maker_coin_platform": transform.get_coin_platform(i["maker_coin"]),
                    "taker_coin_ticker": transform.strip_coin_platform(i["taker_coin"]),
                    "taker_coin_platform": transform.get_coin_platform(i["taker_coin"]),
                    "price": Decimal(i["taker_amount"] / i["maker_amount"]),
                    "reverse_price": Decimal(i["maker_amount"] / i["taker_amount"]),
                }
            )
            if "is_success" not in i:
                if is_success is not None:
                    if is_success:
                        i.update({"is_success": 1})
                    else:
                        i.update({"is_success": 0})
                else:
                    i.update({"is_success": -1})

            for k, v in i.items():
                if k in [
                    "maker_pubkey",
                    "taker_pubkey",
                    "taker_gui",
                    "maker_gui",
                    "taker_version",
                    "maker_version",
                ]:
                    if v in [None, ""]:
                        i.update({k: ""})
                if k in [
                    "maker_coin_usd_price",
                    "taker_coin_usd_price",
                    "started_at",
                    "finished_at",
                ]:
                    if v in [None, ""]:
                        i.update({k: 0})

    except Exception as e:
        return default_error(e)
    msg = "Data normalised"
    return default_result(msg=msg, data=data, loglevel="updated")


@timed
def cipi_to_defi_swap(cipi_data, defi_data=None):
    """
    Compares with existing to select best value where there is a conflict,
    or returns normalised data from a source database with derived fields
    calculated or defaults applied
    """
    try:
        if defi_data is None:
            for i in ["taker_gui", "maker_gui", "taker_version", "maker_version"]:
                if i not in cipi_data:
                    cipi_data.update({i: ""})
            data = DefiSwap(
                uuid=cipi_data["uuid"],
                taker_amount=cipi_data["taker_amount"],
                taker_coin=cipi_data["taker_coin"],
                taker_gui=cipi_data["taker_gui"],
                taker_pubkey=cipi_data["taker_pubkey"],
                taker_version=cipi_data["taker_version"],
                maker_amount=cipi_data["maker_amount"],
                maker_coin=cipi_data["maker_coin"],
                maker_gui=cipi_data["maker_gui"],
                maker_pubkey=cipi_data["maker_pubkey"],
                maker_version=cipi_data["maker_version"],
                started_at=int(cipi_data["started_at"].timestamp()),
                # Not in Cipi's DB, but better than zero.
                finished_at=cipi_data["started_at"],
                # Not in Cipi's DB, but able to derive.
                price=cipi_data["price"],
                reverse_price=cipi_data["reverse_price"],
                is_success=cipi_data["is_success"],
                maker_coin_platform=cipi_data["maker_coin_platform"],
                maker_coin_ticker=cipi_data["maker_coin_ticker"],
                taker_coin_platform=cipi_data["taker_coin_platform"],
                taker_coin_ticker=cipi_data["taker_coin_ticker"],
                # Extra columns
                trade_type=cipi_data["trade_type"],
                pair=cipi_data["pair"],
                last_updated=int(time.time()),
            )
        else:
            for i in [
                "taker_coin",
                "maker_coin",
                "taker_gui",
                "maker_gui",
                "taker_pubkey",
                "maker_pubkey",
                "taker_version",
                "maker_version",
                "taker_coin_ticker",
                "taker_coin_ticker",
                "taker_coin_platform",
                "taker_coin_platform",
            ]:
                if cipi_data[i] != defi_data[i]:
                    if cipi_data[i] in ["", "None", "unknown", None]:
                        cipi_data[i] = defi_data[i]
                    elif defi_data[i] in ["", "None", "unknown", None]:
                        defi_data[i] = cipi_data[i]
                    elif isinstance(defi_data[i], str):
                        if len(defi_data[i]) == 0:
                            defi_data[i] = cipi_data[i]
                        pass
                    else:
                        # This shouldnt happen
                        logger.warning("Mismatch on incoming cipi data vs defi data:")
                        logger.warning(f"{cipi_data[i]} vs {defi_data[i]}")

            data = DefiSwap(
                uuid=cipi_data["uuid"],
                taker_coin=cipi_data["taker_coin"],
                taker_gui=cipi_data["taker_gui"],
                taker_pubkey=cipi_data["taker_pubkey"],
                taker_version=cipi_data["taker_version"],
                maker_coin=cipi_data["maker_coin"],
                maker_gui=cipi_data["maker_gui"],
                maker_pubkey=cipi_data["maker_pubkey"],
                maker_version=cipi_data["maker_version"],
                maker_coin_platform=cipi_data["maker_coin_platform"],
                maker_coin_ticker=cipi_data["maker_coin_ticker"],
                taker_coin_platform=cipi_data["taker_coin_platform"],
                taker_coin_ticker=cipi_data["taker_coin_ticker"],
                taker_amount=max(cipi_data["taker_amount"], defi_data["taker_amount"]),
                maker_amount=max(cipi_data["maker_amount"], defi_data["maker_amount"]),
                started_at=max(
                    int(cipi_data["started_at"].timestamp()), defi_data["started_at"]
                ),
                finished_at=max(
                    int(cipi_data["started_at"].timestamp()), defi_data["finished_at"]
                ),
                is_success=max(cipi_data["is_success"], defi_data["is_success"]),
                # Not in Cipi's DB, but derived from taker/maker amounts.
                price=max(cipi_data["price"], defi_data["price"]),
                reverse_price=max(
                    cipi_data["reverse_price"], defi_data["reverse_price"]
                ),
                # Not in Cipi's DB
                maker_coin_usd_price=max(0, defi_data["maker_coin_usd_price"]),
                taker_coin_usd_price=max(0, defi_data["taker_coin_usd_price"]),
                # Extra columns
                trade_type=defi_data["trade_type"],
                pair=defi_data["pair"],
                last_updated=int(time.time()),
            )
        if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
            data.duration = data.finished_at - data.started_at
        else:
            data.duration = -1
    except Exception as e:
        return default_error(e)
    msg = "cipi to defi conversion complete"
    return default_result(msg=msg, data=data, loglevel="muted")


@timed
def mm2_to_defi_swap(mm2_data, defi_data=None):
    """
    Compares with existing to select best value where there is a conflict
    """
    try:
        if defi_data is None:
            for i in ["taker_gui", "maker_gui", "taker_version", "maker_version"]:
                if i not in mm2_data:
                    mm2_data.update({i: ""})
            data = DefiSwap(
                uuid=mm2_data["uuid"],
                taker_amount=mm2_data["taker_amount"],
                taker_coin=mm2_data["taker_coin"],
                taker_pubkey=mm2_data["taker_pubkey"],
                maker_amount=mm2_data["maker_amount"],
                maker_coin=mm2_data["maker_coin"],
                maker_pubkey=mm2_data["maker_pubkey"],
                is_success=mm2_data["is_success"],
                maker_coin_platform=mm2_data["maker_coin_platform"],
                maker_coin_ticker=mm2_data["maker_coin_ticker"],
                taker_coin_platform=mm2_data["taker_coin_platform"],
                taker_coin_ticker=mm2_data["taker_coin_ticker"],
                started_at=mm2_data["started_at"],
                finished_at=mm2_data["finished_at"],
                # Not in MM2 DB, but able to derive.
                price=mm2_data["price"],
                reverse_price=mm2_data["reverse_price"],
                # Not in MM2 DB, using default.
                taker_gui=mm2_data["taker_gui"],
                taker_version=mm2_data["taker_version"],
                maker_gui=mm2_data["maker_gui"],
                maker_version=mm2_data["maker_version"],
                # Extra columns
                trade_type=mm2_data["trade_type"],
                pair=mm2_data["pair"],
                last_updated=int(time.time()),
            )
        else:
            for i in [
                "taker_coin",
                "maker_coin",
                "taker_pubkey",
                "maker_pubkey",
                "taker_coin_ticker",
                "taker_coin_ticker",
                "taker_coin_platform",
                "taker_coin_platform",
            ]:
                if mm2_data[i] != defi_data[i]:
                    if mm2_data[i] in ["", "None", None, -1, "unknown"]:
                        mm2_data[i] = defi_data[i]
                    elif defi_data[i] in ["", "None", None, -1, "unknown"]:
                        pass
                    elif isinstance(mm2_data[i], str):
                        if len(mm2_data[i]) == 0:
                            mm2_data[i] = defi_data[i]
                    else:
                        # This shouldnt happen
                        logger.warning("Mismatch on incoming mm2 data vs defi data:")
                        logger.warning(f"{mm2_data[i]} vs {defi_data[i]}")
                        logger.warning(f"{type(mm2_data[i])} vs {type(defi_data[i])}")
            data = DefiSwap(
                uuid=mm2_data["uuid"],
                taker_coin=mm2_data["taker_coin"],
                taker_pubkey=mm2_data["taker_pubkey"],
                maker_coin=mm2_data["maker_coin"],
                maker_pubkey=mm2_data["maker_pubkey"],
                maker_coin_platform=mm2_data["maker_coin_platform"],
                maker_coin_ticker=mm2_data["maker_coin_ticker"],
                taker_coin_platform=mm2_data["taker_coin_platform"],
                taker_coin_ticker=mm2_data["taker_coin_ticker"],
                taker_amount=max(mm2_data["taker_amount"], defi_data["taker_amount"]),
                maker_amount=max(mm2_data["maker_amount"], defi_data["maker_amount"]),
                started_at=max(mm2_data["started_at"], defi_data["started_at"]),
                finished_at=max(mm2_data["finished_at"], defi_data["finished_at"]),
                is_success=max(mm2_data["is_success"], defi_data["is_success"]),
                maker_coin_usd_price=max(0, defi_data["maker_coin_usd_price"]),
                taker_coin_usd_price=max(0, defi_data["taker_coin_usd_price"]),
                # Not in MM2 DB, but derived from taker/maker amounts.
                price=max(mm2_data["price"], defi_data["price"]),
                reverse_price=max(
                    mm2_data["reverse_price"], defi_data["reverse_price"]
                ),
                # Not in MM2 DB, keep existing value
                taker_gui=defi_data["taker_gui"],
                maker_gui=defi_data["maker_gui"],
                taker_version=defi_data["taker_version"],
                maker_version=defi_data["maker_version"],
                # Extra columns
                trade_type=defi_data["trade_type"],
                pair=defi_data["pair"],
                last_updated=int(time.time()),
            )
        if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
            data.duration = data.finished_at - data.started_at
        else:
            data.duration = -1

    except Exception as e:
        return default_error(e)
    msg = "mm2 to defi conversion complete"
    return default_result(msg=msg, data=data, loglevel="muted")


@timed
def import_cipi_swaps(
    pgdb: SqlDB,
    pgdb_query: SqlQuery,
    start=int(time.time() - 86400),
    end=int(time.time()),
):
    try:
        # import Cipi's swap data
        ext_mysql = SqlQuery("mysql")
        cipi_swaps = ext_mysql.get_swaps(CipiSwap, start_time=start, end_time=end)
        cipi_swaps = normalise_swap_data(cipi_swaps, ext_mysql.gecko_source)
        if len(cipi_swaps) > 0:
            with Session(pgdb.engine) as session:
                count = pgdb_query.get_count(DefiSwap.uuid)
                cipi_swaps_data = {}
                for i in cipi_swaps:
                    cipi_swaps_data.update({i["uuid"]: i})
                overlapping_swaps = (
                    session.query(DefiSwap)
                    .filter(DefiSwap.uuid.in_(cipi_swaps_data.keys()))
                    .all()
                )

                updates = []
                for each in overlapping_swaps:
                    # Get dict row for ex         "isting swaps
                    cipi_data = cipi_to_defi_swap(
                        cipi_swaps_data[each.uuid], each.__dict__
                    ).__dict__
                    # create bindparam
                    cipi_data.update({"_id": each.id})
                    # remove id field to avoid contraint errors
                    if "_sa_instance_state" in cipi_data:
                        del cipi_data["_sa_instance_state"]
                    if "id" in cipi_data:
                        del cipi_data["id"]
                    # all to bulk update list
                    updates.append(cipi_data)
                    # remove from processing queue
                    cipi_swaps_data.pop(each.uuid)

                if len(updates) > 0:
                    # Update existing records
                    bind_values = {
                        i: bindparam(i) for i in updates[0].keys() if i not in ["_id"]
                    }
                    stmt = (
                        update(DefiSwap)
                        .where(DefiSwap.id == bindparam("_id"))
                        .values(bind_values)
                    )
                    session.execute(stmt, updates)

                # Add new records left in processing queue
                for uuid in cipi_swaps_data.keys():
                    swap = cipi_to_defi_swap(cipi_swaps_data[uuid])
                    if "_sa_instance_state" in swap:
                        del swap["_sa_instance_state"]
                    if "id" in swap:
                        del swap["id"]
                    if uuid not in updates:
                        session.add(swap)
                session.commit()
                count_after = pgdb_query.get_count(DefiSwap.uuid)
                msg = f"{count_after - count} records added from Cipi database"
                msg = f" | {len(updates)} records updated from Cipi database"
        else:
            msg = "Zero Cipi swaps returned!"

    except Exception as e:
        return default_error(e)
    return default_result(msg=msg, loglevel="updated")


@timed
def import_mm2_swaps(
    pgdb: SqlDB,
    pgdb_query: SqlQuery,
    start=int(time.time() - 86400),
    end=int(time.time()),
):
    try:
        # Import in Sqlite (all) database
        mm2_sqlite = SqlQuery("sqlite", db_path=MM2_DB_PATH_ALL)
        mm2_swaps = mm2_sqlite.get_swaps(StatsSwap, start_time=start, end_time=end)
        mm2_swaps = normalise_swap_data(mm2_swaps, mm2_sqlite.gecko_source)
        if len(mm2_swaps) > 0:
            with Session(pgdb.engine) as session:
                count = pgdb_query.get_count(DefiSwap.uuid)
                mm2_swaps_data = {}
                for i in mm2_swaps:
                    mm2_swaps_data.update({i["uuid"]: i})

                overlapping_swaps = (
                    session.query(DefiSwap)
                    .filter(DefiSwap.uuid.in_(mm2_swaps_data.keys()))
                    .all()
                )

                updates = []
                for each in overlapping_swaps:
                    # Get dict row for existing swaps
                    mm2_data = mm2_to_defi_swap(
                        mm2_swaps_data[each.uuid], each.__dict__
                    ).__dict__
                    # create bindparam
                    mm2_data.update({"_id": each.id})
                    # remove id field to avoid contraint errors
                    if "id" in mm2_data:
                        del mm2_data["id"]
                    if "_sa_instance_state" in mm2_data:
                        del mm2_data["_sa_instance_state"]
                    # all to bulk update list
                    updates.append(mm2_data)
                    # remove from processing queue
                    mm2_swaps_data.pop(each.uuid)

                if len(updates) > 0:
                    # Update existing records
                    bind_values = {
                        i: bindparam(i) for i in updates[0].keys() if i not in ["_id"]
                    }
                    stmt = (
                        update(DefiSwap)
                        .where(DefiSwap.id == bindparam("_id"))
                        .values(bind_values)
                    )
                    session.execute(stmt, updates)

                # Add new records left in processing queue
                for uuid in mm2_swaps_data.keys():
                    swap = mm2_to_defi_swap(mm2_swaps_data[uuid])
                    if "_sa_instance_state" in swap:
                        del swap["_sa_instance_state"]
                    if "id" in swap:
                        del swap["id"]
                    if uuid not in updates:
                        session.add(swap)
                session.commit()
                count_after = pgdb_query.get_count(DefiSwap.uuid)
                msg = f"{count_after - count} records added from MM2.db"
                msg += f" | {len(updates)} records updated from MM2.db"
        else:
            msg = "Zero MM2 swaps returned!"
    except Exception as e:
        return default_error(e)
    return default_result(msg=msg, loglevel="updated")


@timed
def populate_pgsqldb(start=int(time.time() - 86400), end=int(time.time())):
    try:
        pgdb = SqlUpdate("pgsql")
        pgdb_query = SqlQuery("pgsql")
        import_cipi_swaps(pgdb, pgdb_query, start=start, end=end)
        import_mm2_swaps(pgdb, pgdb_query, start=start, end=end)
    except Exception as e:
        return default_error(e)
    msg = f"Importing swaps from {start} - {end} complete"
    return default_result(msg=msg, loglevel="updated")


def reset_defi_stats_table():
    pgdb = SqlUpdate("pgsql")
    pgdb.drop("defi_swaps")
    SQLModel.metadata.create_all(pgdb.engine)
    logger.merge("Recreated PGSQL Table")
    pgdb_query = SqlQuery("pgsql")
    logger.merge(pgdb_query.get_count(StatsSwap.uuid))


def get_tablename(table):
    if isinstance(table, str):
        return table
    else:
        return table.__tablename__


if __name__ == "__main__":
    pgsql = SqlUpdate("pgsql")