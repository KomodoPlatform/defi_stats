#!/usr/bin/env python3
from decimal import Decimal
from enum import Enum
from datetime import datetime, timezone
from typing import Dict
from itertools import chain
from dotenv import load_dotenv
from sqlalchemy.sql.expression import bindparam
from sqlmodel import Session, SQLModel, create_engine, text, update, select, or_
from sqlalchemy import Numeric, func

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
    IS_TESTING,
)
from db.schema import DefiSwap, DefiSwapTest, StatsSwap, CipiSwap, CipiSwapFailed
from lib.coins import get_gecko_price
from lib.cache import CacheItem
from util.exceptions import InvalidParamCombination
from util.logger import logger, timed
from util.transform import merge, sortdata
import util.cron as cron
import util.defaults as default
import util.helper as helper
import util.memcache as memcache
import util.templates as template
import util.transform as transform
import util.validate as validate

load_dotenv()


class SqlDB:
    def __init__(
        self,
        db_type="pgsql",
        db_path=None,
        external=False,
    ) -> None:
        self.db_type = db_type
        self.db_path = db_path
        self.external = external
        if self.db_type == "pgsql":
            self.host = POSTGRES_HOST
            self.user = POSTGRES_USERNAME
            self.password = POSTGRES_PASSWORD
            self.port = POSTGRES_PORT
            if IS_TESTING:
                self.table = DefiSwapTest
            else:
                self.table = DefiSwap
            self.db_url = (
                f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}"
            )
        elif self.db_type == "sqlite":
            if self.db_path is not None:
                self.table = StatsSwap
                self.db_path = db_path
                self.db_url = f"sqlite:///{self.db_path}"

        elif self.db_type == "mysql":
            self.external = True
            self.table = CipiSwap
            self.host = MYSQL_HOSTNAME
            self.user = MYSQL_USERNAME
            self.password = MYSQL_PASSWORD
            self.port = MYSQL_PORT
            self.database = MYSQL_DATABASE
            self.db_url = f"mysql://{self.user}:{self.password}@{self.host}:{self.port}"
            self.db_url += f"/{self.database}"

        self.engine = create_engine(self.db_url)  # ), echo=True)
        self.sqlfilter = SqlFilter(self.table)


class SqlFilter:
    def __init__(self, table=DefiSwap) -> None:
        self.table = table

    def coin(self, q, coin):
        if coin is not None:
            q = q.filter(
                or_(
                    coin == self.table.maker_coin,
                    coin == self.table.taker_coin,
                )
            )
        return q

    def gui(self, q, gui):
        if gui is not None:
            q = q.filter(
                or_(
                    gui == self.table.maker_gui,
                    gui == self.table.taker_gui,
                )
            )
        return q

    def pair(self, q, pair):
        if pair is not None:
            pair = transform.strip_pair_platforms(pair)
            q = q.filter(
                or_(
                    pair == self.table.pair_std,
                    pair == self.table.pair_std_reverse,
                )
            )
        return q

    def pubkey(self, q, pubkey):
        if pubkey is not None:
            q = q.filter(
                or_(
                    pubkey == self.table.pubkey_gui,
                    pubkey == self.table.pubkey_gui,
                )
            )
        return q

    def success(self, q, success_only=True, failed_only=False):
        if failed_only:
            if self.table == CipiSwap:
                return []
            elif self.table != CipiSwapFailed:
                return q.filter(self.table.is_success == 0)
        if success_only:
            if self.table == CipiSwapFailed:
                return []
            elif self.table != CipiSwap:
                return q.filter(self.table.is_success == 1)
        return q

    def timestamps(self, q, start_time, end_time):
        if self.table in [CipiSwap, CipiSwapFailed]:
            q = q.filter(
                self.table.started_at > start_time, self.table.started_at < end_time
            )
        else:
            q = q.filter(
                self.table.finished_at > start_time, self.table.finished_at < end_time
            )
        return q

    def version(self, q, version):
        if version is not None:
            q = q.filter(
                or_(
                    version == self.table.version_gui,
                    version == self.table.version_gui,
                )
            )
        return q


class SqlUpdate(SqlDB):
    def __init__(
        self,
        db_type="pgsql",
        db_path=None,
        external=False,
    ) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)

    def drop(self, table):
        try:
            with Session(self.engine) as session:
                session.exec(text(f"DROP TABLE {get_tablename(table)};"))
                session.commit()
        except Exception as e:
            logger.warning(e)


class SqlQuery(SqlDB):
    def __init__(
        self,
        db_type="pgsql",
        db_path=None,
        external=False,
        with_enums=False,
    ) -> None:
        SqlDB.__init__(self, db_type=db_type, db_path=db_path, external=external)
        if with_enums:
            self.enums = self.get_enums()
            self.no_distinct_cols = [
                "duration",
                "finished_at",
                "id",
                "last_updated",
                "maker_amount",
                "maker_coin_usd_price",
                "price",
                "reverse_price",
                "started_at",
                "taker_amount",
                "taker_coin_usd_price",
                "uuid",
            ]

    @property
    def ValidTickers(self):
        return Enum(
            "ValidTickers",
            {i: i for i in self.enums["tickers"]},
            type=str,
        )

    @property
    def ValidPlatforms(self):
        return Enum(
            "ValidPlatforms",
            {i: i for i in self.enums["platforms"]},
            type=str,
        )

    @property
    def DefiSwapColumns(self):
        return Enum(
            "DefiSwapColumns",
            {i: i for i in self.enums["defi_swap_cols"]},
            type=str,
        )

    @property
    def DefiSwapColumnsDistinct(self):
        return Enum(
            "DefiSwapColumnsDistinct",
            {
                i: i
                for i in self.enums["defi_swap_cols"]
                if i not in self.no_distinct_cols
            },
            type=str,
        )

    @property
    def ValidGuis(self):
        return Enum(
            "ValidGuis",
            {i: i for i in self.enums["guis"] if i not in ["", None]},
            type=str,
        )

    @property
    def ValidPairs(self):
        return Enum(
            "ValidPairs",
            {i: i for i in self.enums["pairs"] if i not in ["", None]},
            type=str,
        )

    @property
    def ValidPubkeys(self):
        return Enum(
            "ValidPubkeys",
            {i: i for i in self.enums["pubkeys"] if i not in ["", None]},
            type=str,
        )

    @property
    def ValidVersions(self):
        return Enum(
            "ValidVersions",
            {i: i for i in self.enums["versions"] if i not in ["", None]},
            type=str,
        )

    @property
    def ValidCoins(self):
        coins_config = memcache.get_coins_config()
        data = sorted([j for j in coins_config.keys()])
        return Enum("ValidCoins", {i: i for i in data}, type=str)

    # TODO: Subclass 'volumes'
    @timed
    def coin_trade_volumes(
        self,
        start_time: int = 0,
        end_time: int = 0,
        pubkey: str | None = None,
        gui: str | None = None,
        version: str | None = None,
    ) -> list:
        """
        Returns volume traded of coin between two timestamps.
        If no timestamp is given, returns data for last 24 hrs.
        """
        try:
            if start_time == 0:
                start_time = int(cron.now_utc()) - 86400
            if end_time == 0:
                end_time = int(cron.now_utc())

            resp = {}
            total_swaps = 0

            with Session(self.engine) as session:
                q = session.query(
                    func.sum(func.cast(self.table.maker_amount, Numeric)).label(
                        "maker_volume"
                    ),
                    self.table.maker_coin.label("coin"),
                    self.table.maker_coin_ticker.label("ticker"),
                    func.count(self.table.maker_coin).label("num_swaps"),
                )

                q = self.sqlfilter.success(q)
                q = self.sqlfilter.timestamps(q, start_time, end_time)
                q = self.sqlfilter.gui(q, gui)
                q = self.sqlfilter.version(q, version)
                q = self.sqlfilter.pubkey(q, pubkey)
                q = q.group_by(self.table.maker_coin, self.table.maker_coin_ticker)
                q = q.order_by(
                    self.table.maker_coin, self.table.maker_coin_ticker.desc()
                )
                data = [dict(i) for i in q.all()]

            for i in data:
                coin = i["coin"]
                ticker = i["ticker"]
                num_swaps = Decimal(i["num_swaps"])
                maker_vol = Decimal(i["maker_volume"])

                if ticker not in resp:
                    resp.update({ticker: {"ALL": template.coin_trade_vol_item()}})

                if coin not in resp[ticker]:
                    resp[ticker].update({coin: template.coin_trade_vol_item()})

                total_swaps += num_swaps
                resp[ticker]["ALL"]["swaps"] += num_swaps
                resp[ticker][coin]["swaps"] += num_swaps

                resp[ticker]["ALL"]["maker_volume"] += maker_vol
                resp[ticker][coin]["maker_volume"] += maker_vol

                resp[ticker]["ALL"]["trade_volume"] += maker_vol
                resp[ticker][coin]["trade_volume"] += maker_vol

            with Session(self.engine) as session:
                q = session.query(
                    func.sum(func.cast(self.table.taker_amount, Numeric)).label(
                        "taker_volume"
                    ),
                    self.table.taker_coin.label("coin"),
                    self.table.taker_coin_ticker.label("ticker"),
                    func.count(self.table.taker_coin).label("num_swaps"),
                )

                q = self.sqlfilter.success(q)
                q = self.sqlfilter.timestamps(q, start_time, end_time)
                q = self.sqlfilter.gui(q, gui)
                q = self.sqlfilter.version(q, version)
                q = self.sqlfilter.pubkey(q, pubkey)
                q = q.group_by(self.table.taker_coin, self.table.taker_coin_ticker)
                q = q.order_by(
                    self.table.taker_coin, self.table.taker_coin_ticker.desc()
                )
                data = [dict(i) for i in q.all()]

            for i in data:
                coin = i["coin"]
                ticker = i["ticker"]
                num_swaps = Decimal(i["num_swaps"])
                taker_vol = Decimal(i["taker_volume"])

                if ticker not in resp:
                    resp.update({ticker: {"ALL": template.coin_trade_vol_item()}})

                if coin not in resp[ticker]:
                    resp[ticker].update({coin: template.coin_trade_vol_item()})

                total_swaps += num_swaps
                resp[ticker]["ALL"]["swaps"] += num_swaps
                resp[ticker][coin]["swaps"] += num_swaps

                resp[ticker]["ALL"]["taker_volume"] += taker_vol
                resp[ticker][coin]["taker_volume"] += taker_vol

                resp[ticker]["ALL"]["trade_volume"] += taker_vol
                resp[ticker][coin]["trade_volume"] += taker_vol

            data = {
                "start_time": start_time,
                "end_time": end_time,
                "range_days": (end_time - start_time) / 86400,
                "swaps": num_swaps,
                "volumes": resp,
            }
            return default.result(
                data=data, msg="coin_trade_volumes complete", loglevel="debug"
            )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def coin_trade_volumes_usd(self, volumes: Dict) -> list:
        """
        Returns volume traded of coin between two timestamps.
        If no timestamp is given, returns volume for last 24hrs.
        Price is based on current price, so less accurate for
        longer timespans
        """
        try:
            total_swaps = 0
            total_maker_vol_usd = 0
            total_taker_vol_usd = 0
            for ticker in volumes["volumes"]:
                usd_price = get_gecko_price(ticker)
                for coin in volumes["volumes"][ticker]:
                    taker_vol = volumes["volumes"][ticker][coin]["taker_volume"]
                    maker_vol = volumes["volumes"][ticker][coin]["maker_volume"]
                    taker_vol_usd = taker_vol * usd_price
                    maker_vol_usd = maker_vol * usd_price
                    total_trade_vol_usd = taker_vol_usd + maker_vol_usd

                    volumes["volumes"][ticker][coin].update(
                        {
                            "taker_volume_usd": taker_vol_usd,
                            "maker_volume_usd": maker_vol_usd,
                            "trade_volume_usd": total_trade_vol_usd,
                        }
                    )
                    total_maker_vol_usd += maker_vol_usd
                    total_taker_vol_usd += taker_vol_usd
                total_swaps += volumes["volumes"][ticker]["ALL"]["swaps"]

            total_trade_vol_usd = total_maker_vol_usd + total_taker_vol_usd
            # global coin swaps divided by two wrt both coins in pair
            global_totals = {
                "swaps": total_swaps / 2,
                "taker_volume_usd": total_taker_vol_usd,
                "maker_volume_usd": total_maker_vol_usd,
                "trade_volume_usd": total_trade_vol_usd,
            }
            volumes.update(global_totals)
            return volumes

        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def pair_trade_volumes(
        self,
        start_time: int = 0,
        end_time: int = 0,
        pubkey: str | None = None,
        gui: str | None = None,
        coin: str | None = None,
        version: str | None = None,
    ) -> list:
        """
        Returns volume traded of pairs between two timestamps.
        If no timestamp is given, returns data for last 24 hrs.
        """
        try:
            if start_time == 0:
                start_time = int(cron.now_utc()) - 86400
            if end_time == 0:
                end_time = int(cron.now_utc())

            resp = {}
            num_swaps = 0

            with Session(self.engine) as session:
                q = session.query(
                    self.table.pair,
                    self.table.trade_type,
                    func.sum(func.cast(self.table.maker_amount, Numeric)).label(
                        "maker_volume"
                    ),
                    func.sum(func.cast(self.table.taker_amount, Numeric)).label(
                        "taker_volume"
                    ),
                    func.count(self.table.maker_amount).label("num_swaps"),
                )

                q = self.sqlfilter.success(q)
                q = self.sqlfilter.timestamps(q, start_time, end_time)
                q = self.sqlfilter.gui(q, gui)
                q = self.sqlfilter.version(q, version)
                q = self.sqlfilter.pubkey(q, pubkey)
                q = self.sqlfilter.coin(q, coin)
                q = q.group_by(self.table.pair, self.table.trade_type)
                q = q.order_by(self.table.pair.asc())
                data = [dict(i) for i in q.all()]

            for i in data:
                pair_std = i["pair_std"]
                if pair_std not in resp:
                    resp.update({pair_std: {"ALL": template.pair_trade_vol_item()}})
                if i["pair"] not in resp[pair_std]:
                    resp[pair_std].update({i["pair"]: template.pair_trade_vol_item()})

                num_swaps = Decimal(i["num_swaps"])
                if i["trade_type"] == "buy":
                    base_vol = Decimal(i["maker_volume"])
                    quote_vol = Decimal(i["taker_volume"])

                elif i["trade_type"] == "sell":
                    base_vol = Decimal(i["taker_volume"])
                    quote_vol = Decimal(i["maker_volume"])

                resp[pair_std]["ALL"]["swaps"] += num_swaps
                resp[pair_std]["ALL"]["base_volume"] += base_vol
                resp[pair_std]["ALL"]["quote_volume"] += quote_vol
                resp[pair_std][i["pair"]]["swaps"] += num_swaps
                resp[pair_std][i["pair"]]["base_volume"] += base_vol
                resp[pair_std][i["pair"]]["quote_volume"] += quote_vol

            data = {
                "start_time": start_time,
                "end_time": end_time,
                "range_days": (end_time - start_time) / 86400,
                "swaps": num_swaps,
                "volumes": resp,
            }
            return default.result(data=data, msg="pair_trade_volumes", loglevel="query")
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def pair_trade_volumes_usd(self, volumes: Dict) -> list:
        """
        Returns volume traded of a pair between two timestamps.
        If no timestamp is given, returns volume for last 24hrs.
        Price is based on current price, so less accurate for
        longer timespans
        """
        try:
            total_swaps = 0
            total_base_vol_usd = 0
            total_quote_vol_usd = 0
            for pair_std in volumes["volumes"]:
                base, quote = helper.base_quote_from_pair(pair_std)
                base_usd_price = get_gecko_price(base)
                quote_usd_price = get_gecko_price(quote)

                for pair in volumes["volumes"][pair_std]:
                    base_vol = volumes["volumes"][pair_std][pair]["base_volume"]
                    quote_vol = volumes["volumes"][pair_std][pair]["quote_volume"]
                    base_vol_usd = base_vol * base_usd_price
                    quote_vol_usd = quote_vol * quote_usd_price
                    total_trade_vol_usd = base_vol_usd + quote_vol_usd
                    volumes["volumes"][pair_std][pair].update(
                        {
                            "dex_price": base_vol / quote_vol,
                            "base_volume_usd": base_vol_usd,
                            "quote_volume_usd": quote_vol_usd,
                            "trade_volume_usd": total_trade_vol_usd,
                        }
                    )

                total_swaps += volumes["volumes"][pair_std]["ALL"]["swaps"]
                total_base_vol_usd += volumes["volumes"][pair_std]["ALL"][
                    "base_volume_usd"
                ]
                total_quote_vol_usd += volumes["volumes"][pair_std]["ALL"][
                    "quote_volume_usd"
                ]

            total_trade_vol_usd = total_base_vol_usd + total_quote_vol_usd
            volumes.update(
                {
                    "swaps": total_swaps,
                    "base_volume_usd": total_base_vol_usd,
                    "quote_volume_usd": total_quote_vol_usd,
                    "trade_volume_usd": total_trade_vol_usd,
                }
            )
            return default.result(
                data=volumes, msg="pair_trade_volumes_usd complete", loglevel="query"
            )

        except Exception as e:  # pragma: no cover
            return default.error(e)

    # TODO: Pair swap duration stats.
    # Fastest, slowest, average, [x,y] for graph
    # TODO: Subclass 'last trade'
    @timed
    def last_trade(self, group_by_cols, is_success: bool = True):
        try:
            with Session(self.engine) as session:
                resp = {}
                # 1st query for most recent swap info for category
                category = list(
                    chain.from_iterable((obj, "-") for obj in group_by_cols[:-1])
                ) + [group_by_cols[-1]]
                cols = [
                    self.table.uuid.label("last_swap_uuid"),
                    self.table.finished_at.label("last_swap_time"),
                    self.table.price.label("last_swap_price"),
                    func.concat(*category).label("category"),
                ]
                q = session.query(*cols)
                q = self.sqlfilter.success(q, is_success)
                distinct = q.distinct(*group_by_cols)
                last = distinct.order_by(*group_by_cols, self.table.finished_at.desc())
                last_data = [dict(i) for i in last.all()]

                last_data = {i["category"]: i for i in last_data}
                for pair in last_data:
                    if pair not in resp:
                        resp.update({pair: {}})
                    for k, v in last_data[pair].items():
                        if k != "category":
                            resp[pair].update({k: v})

                # 2nd query for swap first swap info for category
                cols = [
                    self.table.uuid.label("first_swap_uuid"),
                    self.table.finished_at.label("first_swap_time"),
                    self.table.price.label("first_swap_price"),
                    func.concat(*category).label("category"),
                ]
                q = session.query(*cols)
                q = self.sqlfilter.success(q, is_success)
                distinct = q.distinct(*group_by_cols)
                first = distinct.order_by(*group_by_cols, self.table.finished_at.asc())
                first_data = [dict(i) for i in first.all()]
                first_data = {i["category"]: i for i in first_data}
                for pair in first_data:
                    if pair not in resp:
                        resp.update({pair: {}})
                    for k, v in first_data[pair].items():
                        if k != "category":
                            resp[pair].update({k: v})

                return default.result(
                    data=resp,
                    msg="last_traded complete",
                    loglevel="query",
                    ignore_until=5,
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def gui_last_traded(self, is_success: bool = True, min_swaps: int = 0):
        try:
            maker_data = self.last_trade(
                is_success=is_success,
                group_by_cols=[self.table.maker_gui, self.table.maker_version],
            )
            taker_data = self.last_trade(
                is_success=is_success,
                group_by_cols=[self.table.taker_gui, self.table.taker_version],
            )
            data = {}
            for i in maker_data:
                k = transform.derive_app(maker_data[i]["category"])
                if k not in data:
                    data.update({k: template.last_traded_item()})
                data[k].update(
                    {
                        "maker_num_swaps": maker_data[i]["num_swaps"],
                        "maker_last_swap_uuid": maker_data[i]["last_swap_uuid"],
                        "maker_last_swap_time": maker_data[i]["last_swap_time"],
                        "maker_first_swap_uuid": maker_data[i]["first_swap_uuid"],
                        "maker_first_swap_time": maker_data[i]["first_swap_time"],
                        "raw_category": maker_data[i]["category"],
                    }
                )
                data[k]["total_num_swaps"] += maker_data[i]["num_swaps"]

            for i in taker_data:
                k = transform.derive_app(taker_data[i]["category"])
                if k not in data:
                    data.update({k: template.last_traded_item()})
                data[k].update(
                    {
                        "taker_num_swaps": taker_data[i]["num_swaps"],
                        "taker_last_swap_uuid": taker_data[i]["last_swap_uuid"],
                        "taker_last_swap_time": taker_data[i]["last_swap_time"],
                        "taker_first_swap_uuid": taker_data[i]["first_swap_uuid"],
                        "taker_first_swap_time": taker_data[i]["first_swap_time"],
                        "raw_category": taker_data[i]["category"],
                    }
                )
                data[k]["total_num_swaps"] += taker_data[i]["num_swaps"]

            data = {i: data[i] for i in data if data[i]["total_num_swaps"] > min_swaps}
            # Convert the results to a list of dictionaries
            return default.result(
                data=data,
                msg="gui_last_traded complete",
                loglevel="query",
                ignore_until=5,
            )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def pair_last_trade(self, is_success: bool = True):
        try:
            with Session(self.engine) as session:
                group_by_cols = [self.table.pair]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results,
                    msg="pair_last_trade complete",
                    loglevel="query",
                    ignore_until=5,
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def coin_last_traded(self, is_success: bool = True, trade_side: str = "maker"):
        try:
            with Session(self.engine) as session:
                if trade_side == "maker":
                    group_by_cols = [self.table.maker_coin]
                elif trade_side == "taker":
                    group_by_cols = [self.table.taker_coin]
                elif trade_side == "all":
                    group_by_cols = [self.table.maker_coin, self.table.taker_coin]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results,
                    msg="coin_last_traded complete",
                    loglevel="query",
                    ignore_until=5,
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def ticker_last_traded(self, is_success: bool = True, trade_side: str = "maker"):
        try:
            with Session(self.engine) as session:
                if trade_side == "maker":
                    group_by_cols = [self.table.maker_ticker]
                elif trade_side == "taker":
                    group_by_cols = [self.table.taker_ticker]
                elif trade_side == "all":
                    group_by_cols = [self.table.maker_ticker, self.table.taker_ticker]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results, msg="ticker_last_traded complete", loglevel="query"
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def platform_last_traded(self, is_success: bool = True, trade_side: str = "maker"):
        try:
            with Session(self.engine) as session:
                if trade_side == "maker":
                    group_by_cols = [self.table.maker_platform]
                elif trade_side == "taker":
                    group_by_cols = [self.table.taker_platform]
                elif trade_side == "all":
                    group_by_cols = [
                        self.table.maker_platform,
                        self.table.taker_platform,
                    ]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results, msg="platform_last_traded complete", loglevel="query"
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def pubkey_last_traded(self, is_success: bool = True, trade_side: str = "maker"):
        try:
            with Session(self.engine) as session:
                if trade_side == "maker":
                    group_by_cols = [self.table.maker_pubkey]
                elif trade_side == "taker":
                    group_by_cols = [self.table.taker_pubkey]
                elif trade_side == "all":
                    group_by_cols = [self.table.maker_pubkey, self.table.taker_pubkey]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results, msg="pubkey_last_traded complete", loglevel="query"
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def version_last_traded(self, is_success: bool = True, trade_side: str = "maker"):
        try:
            with Session(self.engine) as session:
                if trade_side == "maker":
                    group_by_cols = [self.table.maker_version]
                elif trade_side == "taker":
                    group_by_cols = [self.table.taker_version]
                elif trade_side == "all":
                    group_by_cols = [self.table.maker_version, self.table.taker_version]
                results = self.last_trade(
                    is_success=is_success, group_by_cols=group_by_cols
                )
                return default.result(
                    data=results, msg="version_last_traded complete", loglevel="query"
                )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    # TODO: Subclass 'swaps'
    @timed
    def get_swaps(
        self,
        start_time: int = 0,
        end_time: int = 0,
        limit: int = 100,
        trade_type: int | None = None,
        coin: str | None = None,
        pair: str | None = None,
        pubkey: str | None = None,
        gui: str | None = None,
        version: str | None = None,
        success_only: bool = True,
        failed_only: bool = False,
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
            # TODO: Implement limit and trade_type
            if start_time == 0:
                start_time = int(cron.now_utc()) - 86400
            if end_time == 0:
                end_time = int(cron.now_utc())

            if self.table.__tablename__ in ["swaps", "swaps_failed"]:
                start_time = datetime.fromtimestamp(start_time, timezone.utc)
                end_time = datetime.fromtimestamp(end_time, timezone.utc)
            with Session(self.engine) as session:
                q = select(self.table)
                q = self.sqlfilter.timestamps(q, start_time, end_time)
                q = self.sqlfilter.gui(q, gui)
                q = self.sqlfilter.version(q, version)
                q = self.sqlfilter.pubkey(q, pubkey)
                q = self.sqlfilter.success(q, success_only, failed_only)
                if self.table in [CipiSwap, CipiSwapFailed]:
                    q = q.order_by(self.table.started_at)
                else:
                    q = q.order_by(self.table.finished_at)

                r = session.exec(q)
                data = [dict(i) for i in r]
                if coin is not None:
                    variants = helper.get_coin_variants(coin)
                    resp = {
                        i: [j for j in data if i in [j["taker_coin"], j["maker_coin"]]]
                        for i in variants
                    }
                    all = []
                    for i in resp:
                        all += resp[i]
                    resp.update({"ALL": all})
                elif pair is not None:
                    resp = {}
                    bridge_swap = validate.is_bridge_swap(pair)
                    variants = helper.get_pair_variants(pair)
                    for variant in variants:
                        # exclude duplication for bridge swaps
                        if (
                            bridge_swap
                            and variant != sortdata.order_pair_by_market_cap(variant)
                        ):
                            continue
                        base, quote = helper.base_quote_from_pair(variant)
                        variant_trades = [
                            k
                            for k in data
                            if base in [k["taker_coin"], k["maker_coin"]]
                            and quote in [k["taker_coin"], k["maker_coin"]]
                        ]
                        resp.update({variant: variant_trades})
                    all = []
                    for i in resp:
                        all += resp[i]
                    sortdata.sort_dict_list(data=all, key="finished_at", reverse=True)
                    resp.update({"ALL": all})

                else:
                    resp = data
        except Exception as e:
            return default.error(e)
        msg = f"Got {len(data)} swaps from {self.table.__tablename__}"
        msg += f" between {start_time} and {end_time}"
        return default.result(data=resp, msg=msg, loglevel="muted")

    @timed
    def get_swap(self, uuid: str = ""):
        try:
            with Session(self.engine) as session:
                q = select(self.table).where(self.table.uuid == uuid)
                data = [dict(i) for i in session.exec(q)]
                if len(data) == 0:
                    return {"error": f"swap uuid {uuid} not found"}
                else:
                    return data[0]
        except Exception as e:  # pragma: no cover
            return default.error(e)

    @timed
    def get_timespan_swaps(self, start_time: int = 0, end_time: int = 0) -> list:
        """
        Returns a list of swaps between two timestamps
        """
        try:
            return self.get_swaps(start_time=start_time, end_time=end_time)
        except Exception as e:  # pragma: no cover
            return default.error(e)

    def get_swaps_for_coin(
        self, coin: str, merge_segwit: bool = False, all=False, **kwargs
    ):
        """
        Returns swaps for a variant of a coin only.
        Optionally, segwit coins can be merged
        """
        swaps = self.get_swaps(coin=coin, **kwargs)
        if all:
            return swaps["ALL"]
        if merge_segwit:
            variants = helper.get_coin_variants(coin, segwit_only=True)
            return merge.segwit_swaps(variants, swaps)
        return swaps[coin]

    def get_swaps_for_pair(
        self, base: str, quote: str, merge_segwit: bool = True, all=False, **kwargs
    ):
        """
        Returns swaps for a variant of a pair only.
        Optionally, pairs with segwit coins can be merged
        """
        pair = f"{base}_{quote}"
        swaps = self.get_swaps(pair=pair, **kwargs)
        if all:
            return swaps["ALL"]
        if merge_segwit:
            variants = helper.get_pair_variants(pair, segwit_only=True)
            return merge.segwit_swaps(variants, swaps)
        return swaps[pair]

    @timed
    def swap_uuids(
        self,
        start_time: int = 0,
        end_time: int = 0,
        coin=None,
        pair=None,
        pubkey: str | None = None,
        gui: str | None = None,
        version: str | None = None,
        success_only: bool = True,
        failed_only: bool = False,
    ):
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        swaps = self.get_swaps(
            start_time=start_time,
            end_time=end_time,
            coin=coin,
            pair=pair,
            pubkey=pubkey,
            gui=gui,
            version=version,
            success_only=success_only,
            failed_only=failed_only,
        )

        if coin is not None or pair is not None:
            resp = {}
            for variant in swaps:
                resp.update({variant: [i["uuid"] for i in swaps[variant]]})
            return resp
        else:
            return [i["uuid"] for i in swaps]

    # Subclass under 'utils'
    @timed
    def get_pairs(self, days: int = 7) -> list:
        """
        Returns an alphabetically sorted list of pair strings
        with at least one successful swap in the last 'x' days.
        Results sorted by market cap to conform to CEX standards.
        """
        try:
            start_time = int(cron.now_utc() - 86400 * days)
            end_time = int(cron.now_utc())
            pairs = self.get_distinct(
                column="pair", start_time=start_time, end_time=end_time
            )

            gecko_source = memcache.get_gecko_source()
            if gecko_source is None:
                CacheItem("gecko_source").save()
            # Sort pair by ticker mcap to expose duplicates
            sorted_pairs = list(
                set([sortdata.order_pair_by_market_cap(i) for i in pairs])
            )
            return default.result(
                data=sorted_pairs,
                msg="Got generic pairs from db",
                loglevel="query",
                ignore_until=2,
            )
        except Exception as e:  # pragma: no cover
            return default.error(e)

    def get_distinct(
        self,
        start_time: int = 0,
        end_time: int = 0,
        column: str | None = None,
        coin: str | None = None,
        pair: str | None = None,
        pubkey: str | None = None,
        gui: str | None = None,
        version: str | None = None,
        success_only: bool = True,
        failed_only: bool = False,
    ):
        if end_time == 0:
            end_time = int(cron.now_utc())
        with Session(self.engine) as session:
            if coin is not None:
                q = session.query(self.table.maker_coin, column.taker_coin)
            elif pair is not None:
                q = session.query(self.table.maker_coin, self.table.taker_coin)
            elif column is not None:
                try:
                    col = getattr(self.table, column)
                    q = session.query(col)
                except AttributeError as e:
                    logger.warning(type(e))
                    logger.warning(e)
                    msg = f"'{column}' does not exist in {self.table.__tablename__}!"
                    msg += f" Options are {get_columns(self.table)}"
                    raise InvalidParamCombination(msg=msg)
                except Exception as e:
                    logger.warning(type(e))
                    logger.warning(e)
                    msg = f"'{column}' does not exist in {self.table.__tablename__}!"
                    msg += f" Options are {get_columns(self.table)}"
                    raise InvalidParamCombination(msg=msg)
            else:
                raise InvalidParamCombination(
                    "Unless 'pair' or 'coin' param is set, you must set the 'column' param."
                )

            q = self.sqlfilter.gui(q, gui)
            q = self.sqlfilter.coin(q, coin)
            q = self.sqlfilter.pair(q, pair)
            q = self.sqlfilter.pubkey(q, pubkey)
            q = self.sqlfilter.version(q, version)
            q = self.sqlfilter.timestamps(q, start_time, end_time)
            q = self.sqlfilter.success(q, success_only, failed_only)
            q = q.distinct()
            data = [list(dict(i).values())[0] for i in q.all()]
        return data

    def get_count(
        self,
        start_time: int = 0,
        end_time: int = 0,
        coin: str | None = None,
        pair: str | None = None,
        pubkey: str | None = None,
        gui: str | None = None,
        version: str | None = None,
        success_only: bool = True,
        failed_only: bool = False,
    ):
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())

        with Session(self.engine) as session:
            q = session.query(func.count(self.table.uuid).label("swaps_count"))
            q = self.sqlfilter.gui(q, gui=gui)
            q = self.sqlfilter.coin(q, coin=coin)
            q = self.sqlfilter.pair(q, pair=pair)
            q = self.sqlfilter.pubkey(q, pubkey=pubkey)
            q = self.sqlfilter.version(q, version=version)
            q = self.sqlfilter.timestamps(q, start_time=start_time, end_time=end_time)
            q = self.sqlfilter.success(
                q, success_only=success_only, failed_only=failed_only
            )
            r = q.all()
            return r[0][0]

    def get_last(self, table: str, limit: int = 3):
        try:
            with Session(self.engine) as session:
                r = (
                    session.query(table)
                    .where(table.is_success == 1)
                    .order_by(table.finished_at.desc())
                    .limit(limit)
                    .all()
                )
                return [dict(i) for i in r]
        except Exception as e:
            logger.error(e)

    def get_first(self, table: str, limit: int = 3):
        try:
            with Session(self.engine) as session:
                r = (
                    session.query(table)
                    .where(table.is_success == 1)
                    .order_by(table.finished_at.asc())
                    .limit(limit)
                    .all()
                )
                return [dict(i) for i in r]
        except Exception as e:
            logger.error(e)

    def describe(self, table):
        with Session(self.engine) as session:
            stmt = text(f"DESCRIBE {get_tablename(table)};")
            r = session.exec(stmt)
            for i in r:
                logger.merge(i)

    def get_enums(self):
        coins_config = memcache.get_coins_config()
        return {
            "pairs": sorted(self.get_distinct(column="pair")),
            "tickers": sorted(
                list(set([j.split("-")[0] for j in coins_config.keys()]))
            ),
            "platforms": sorted(
                list(
                    set(
                        [
                            j.split("-")[1]
                            for j in coins_config.keys()
                            if len(j.split("-")) > 1
                        ]
                    )
                )
            ),
            "defi_swap_cols": get_columns(self.table),
            "maker_guis": sorted(self.get_distinct(column="maker_gui")),
            "taker_guis": sorted(self.get_distinct(column="taker_gui")),
            "guis": sorted(
                list(
                    set(
                        self.get_distinct(column="taker_gui")
                        + self.get_distinct(column="maker_gui")
                    )
                )
            ),
            "maker_pubkeys": sorted(self.get_distinct(column="maker_pubkey")),
            "taker_pubkeys": sorted(self.get_distinct(column="taker_pubkey")),
            "pubkeys": sorted(
                list(
                    set(
                        self.get_distinct(column="taker_pubkey")
                        + self.get_distinct(column="maker_pubkey")
                    )
                )
            ),
            "maker_versions": sorted(self.get_distinct(column="maker_version")),
            "taker_versions": sorted(self.get_distinct(column="taker_version")),
            "versions": sorted(
                list(
                    set(
                        self.get_distinct(column="taker_version")
                        + self.get_distinct(column="maker_version")
                    )
                )
            ),
        }

    def swap_counts(self):  # pragma: no cover
        month_ago = int(cron.now_utc()) - 86400 * 30
        fortnight_ago = int(cron.now_utc()) - 86400 * 14
        week_ago = int(cron.now_utc()) - 86400 * 7
        day_ago = int(cron.now_utc()) - 86400
        return {
            "swaps_all_time": self.get_count(start_time=1),
            "swaps_30d": self.get_count(start_time=month_ago),
            "swaps_14d": self.get_count(start_time=fortnight_ago),
            "swaps_7d": self.get_count(start_time=week_ago),
            "swaps_24hr": self.get_count(start_time=day_ago),
        }


class SqlSource:
    def __init__(self) -> None:
        pass

    @timed
    def import_cipi_swaps(
        self,
        pgdb: SqlDB,
        pgdb_query: SqlQuery,
        start_time=int(cron.now_utc() - 86400),
        end_time=int(cron.now_utc()),
    ):
        try:
            # import Cipi's swap data
            ext_mysql = SqlQuery(db_type="mysql")
            cipi_swaps = ext_mysql.get_swaps(start_time=start_time, end_time=end_time)
            cipi_swaps = self.normalise_swap_data(cipi_swaps)
            if len(cipi_swaps) > 0:
                with Session(pgdb.engine) as session:
                    count = pgdb_query.get_count(start_time=1)
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
                        cipi_data = self.cipi_to_defi_swap(
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
                            i: bindparam(i)
                            for i in updates[0].keys()
                            if i not in ["_id"]
                        }
                        stmt = (
                            update(DefiSwap)
                            .where(DefiSwap.id == bindparam("_id"))
                            .values(bind_values)
                        )
                        session.execute(stmt, updates)

                    # Add new records left in processing queue
                    for uuid in cipi_swaps_data.keys():
                        swap = self.cipi_to_defi_swap(cipi_swaps_data[uuid])
                        if "_sa_instance_state" in swap:
                            del swap["_sa_instance_state"]
                        if "id" in swap:
                            del swap["id"]
                        if uuid not in updates:
                            session.add(swap)
                    session.commit()
                    count_after = pgdb_query.get_count(start_time=1)
                    msg = f"{count_after - count} records added from Cipi database"
                    msg = f" | {len(updates)} records updated from Cipi database"
            else:
                msg = "Zero Cipi swaps returned!"

        except Exception as e:
            return default.error(e)
        return default.result(msg=msg, loglevel="updated")

    @timed
    def import_mm2_swaps(
        self,
        pgdb: SqlDB,
        pgdb_query: SqlQuery,
        start_time=int(cron.now_utc() - 86400),
        end_time=int(cron.now_utc()),
    ):
        try:
            # Import in Sqlite (all) database
            mm2_sqlite = SqlQuery(db_type="sqlite", db_path=MM2_DB_PATH_ALL)
            mm2_swaps = mm2_sqlite.get_swaps(start_time=start_time, end_time=end_time)
            mm2_swaps = self.normalise_swap_data(mm2_swaps)
            if len(mm2_swaps) > 0:
                with Session(pgdb.engine) as session:
                    count = pgdb_query.get_count(start_time=1)
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
                        mm2_data = self.mm2_to_defi_swap(
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
                            i: bindparam(i)
                            for i in updates[0].keys()
                            if i not in ["_id"]
                        }
                        stmt = (
                            update(DefiSwap)
                            .where(DefiSwap.id == bindparam("_id"))
                            .values(bind_values)
                        )
                        session.execute(stmt, updates)

                    # Add new records left in processing queue
                    for uuid in mm2_swaps_data.keys():
                        swap = self.mm2_to_defi_swap(mm2_swaps_data[uuid])
                        if "_sa_instance_state" in swap:
                            del swap["_sa_instance_state"]
                        if "id" in swap:
                            del swap["id"]
                        if uuid not in updates:
                            session.add(swap)
                    session.commit()
                    count_after = pgdb_query.get_count(start_time=1)
                    msg = f"{count_after - count} records added from MM2.db"
                    msg += f" | {len(updates)} records updated from MM2.db"
            else:
                msg = "Zero MM2 swaps returned!"
        except Exception as e:
            return default.error(e)
        return default.result(msg=msg, loglevel="updated")

    @timed
    def populate_pgsqldb(
        self,
        start_time=int(cron.now_utc() - 86400),
        end_time=int(cron.now_utc()),
    ):
        try:
            pgdb = SqlUpdate(db_type="pgsql")
            pgdb_query = SqlQuery(db_type="pgsql")
            self.import_cipi_swaps(
                pgdb, pgdb_query, start_time=start_time, end_time=end_time
            )
            self.import_mm2_swaps(
                pgdb, pgdb_query, start_time=start_time, end_time=end_time
            )
            # pgdb_query.describe('defi_swaps')

        except Exception as e:
            return default.error(e)
        msg = f"Importing swaps from {start_time} - {end_time} complete"
        return default.result(msg=msg, loglevel="updated")

    def reset_defi_stats_table(self):
        pgdb = SqlUpdate("pgsql")
        pgdb.drop("defi_swaps")
        SQLModel.metadata.create_all(pgdb.engine)
        logger.merge("Recreated PGSQL Table")

    @timed
    def normalise_swap_data(self, data, is_success=None):
        try:
            for i in data:
                pair_raw = f'{i["maker_coin"]}_{i["taker_coin"]}'
                pair = sortdata.order_pair_by_market_cap(pair_raw)
                pair_reverse = transform.invert_pair(pair)
                pair_std = transform.strip_pair_platforms(pair)
                pair_std_reverse = transform.invert_pair(pair_std)
                if pair_raw == pair:
                    trade_type = "buy"
                    price = Decimal(i["maker_amount"] / i["taker_amount"])
                    reverse_price = Decimal(i["taker_amount"] / i["maker_amount"])
                else:
                    trade_type = "sell"
                    price = Decimal(i["taker_amount"] / i["maker_amount"])
                    reverse_price = Decimal(i["maker_amount"] / i["taker_amount"])
                i.update(
                    {
                        "pair": pair,
                        "pair_std": pair_std,
                        "pair_reverse": pair_reverse,
                        "pair_std_reverse": pair_std_reverse,
                        "trade_type": trade_type,
                        "maker_coin_ticker": transform.strip_coin_platform(
                            i["maker_coin"]
                        ),
                        "maker_coin_platform": transform.get_coin_platform(
                            i["maker_coin"]
                        ),
                        "taker_coin_ticker": transform.strip_coin_platform(
                            i["taker_coin"]
                        ),
                        "taker_coin_platform": transform.get_coin_platform(
                            i["taker_coin"]
                        ),
                        "price": price,
                        "reverse_price": reverse_price,
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
            return default.error(e)
        msg = "Data normalised"
        return default.result(msg=msg, data=data, loglevel="updated")

    @timed
    def cipi_to_defi_swap(self, cipi_data, defi_data=None):
        """
        Compares with existing to select best value where there
        is a conflict, or returns normalised data from a source
        database with derived fields calculated or defaults applied
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
                    pair_reverse=transform.invert_pair(cipi_data["pair"]),
                    pair_std=transform.strip_pair_platforms(cipi_data["pair"]),
                    pair_std_reverse=transform.strip_pair_platforms(
                        transform.invert_pair(cipi_data["pair"])
                    ),
                    last_updated=int(cron.now_utc()),
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
                            logger.warning(
                                "Mismatch on incoming cipi data vs defi data:"
                            )
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
                    taker_amount=max(
                        cipi_data["taker_amount"], defi_data["taker_amount"]
                    ),
                    maker_amount=max(
                        cipi_data["maker_amount"], defi_data["maker_amount"]
                    ),
                    started_at=max(
                        int(cipi_data["started_at"].timestamp()),
                        defi_data["started_at"],
                    ),
                    finished_at=max(
                        int(cipi_data["started_at"].timestamp()),
                        defi_data["finished_at"],
                    ),
                    is_success=max(cipi_data["is_success"], defi_data["is_success"]),
                    # Not in Cipi's DB, derived from taker/maker amounts.
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
                    pair_reverse=transform.invert_pair(cipi_data["pair"]),
                    pair_std=transform.strip_pair_platforms(cipi_data["pair"]),
                    pair_std_reverse=transform.strip_pair_platforms(
                        transform.invert_pair(cipi_data["pair"])
                    ),
                    last_updated=int(cron.now_utc()),
                )
            if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
                data.duration = data.finished_at - data.started_at
            else:
                data.duration = -1
        except Exception as e:
            return default.error(e)
        msg = "cipi to defi conversion complete"
        return default.result(msg=msg, data=data, loglevel="muted")

    @timed
    def mm2_to_defi_swap(self, mm2_data, defi_data=None):
        """
        Compares to select best value where there is a conflict
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
                    pair_reverse=transform.invert_pair(mm2_data["pair"]),
                    pair_std=transform.strip_pair_platforms(mm2_data["pair"]),
                    pair_std_reverse=transform.strip_pair_platforms(
                        transform.invert_pair(mm2_data["pair"])
                    ),
                    last_updated=int(cron.now_utc()),
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
                            logger.warning(
                                "Mismatch on incoming mm2 data vs defi data:"
                            )
                            logger.warning(f"{mm2_data[i]} vs {defi_data[i]}")
                            logger.warning(
                                f"{type(mm2_data[i])} vs {type(defi_data[i])}"
                            )
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
                    taker_amount=max(
                        mm2_data["taker_amount"], defi_data["taker_amount"]
                    ),
                    maker_amount=max(
                        mm2_data["maker_amount"], defi_data["maker_amount"]
                    ),
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
                    pair_reverse=transform.invert_pair(defi_data["pair"]),
                    pair_std=transform.strip_pair_platforms(defi_data["pair"]),
                    pair_std_reverse=transform.strip_pair_platforms(
                        transform.invert_pair(defi_data["pair"])
                    ),
                    last_updated=int(cron.now_utc()),
                )
            if isinstance(data.finished_at, int) and isinstance(data.started_at, int):
                data.duration = data.finished_at - data.started_at
            else:
                data.duration = -1

        except Exception as e:
            return default.error(e)
        msg = "mm2 to defi conversion complete"
        return default.result(msg=msg, data=data, loglevel="muted")


# Subclass 'utils' under SqlQuery
def get_tablename(table):
    if isinstance(table, str):
        return table
    else:
        return table.__tablename__


def get_columns(table: object):
    return sorted(list(table.__annotations__.keys()))


if __name__ == "__main__":
    pgsql = SqlUpdate("pgsql")


# pubkey swaps [maker/taker]
# version swaps [maker/taker]
# gui swaps [maker/taker]
# pubkey versions
# gui versions
# version pubkeys
# gui pubkeys

# swap durations per pair

# fails: find patterns
# - same coin? [maker/taker]
# - same pair? [maker/taker]
# - same pubkey? [maker/taker] Derive reputation score?
# - same version? [maker/taker]
# - same gui? [maker/taker]
