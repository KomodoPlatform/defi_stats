#!/usr/bin/env python3
from lib.generics import Generics
from util.files import Files
from lib.external import CoinGeckoAPI
from db.sqlitedb import get_sqlite_db_paths, get_sqlite_db
from util.logger import timed, logger
from util.defaults import default_error, set_params


class Markets:
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid", "exclude_unpriced"]
            set_params(self, self.kwargs, self.options)
            self.db_path = get_sqlite_db_paths(netid=self.netid)
            self.files = Files(netid=self.netid, testing=self.testing)
            self.gecko = CoinGeckoAPI(testing=self.testing)
            self.generics = Generics(**kwargs)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")

    @timed
    def pairs(self, days=120):
        try:
            data = self.generics.traded_pairs(
                days=120, include_all_kmd=True, exclude_unpriced=False
            )
            # logger.loop(data)
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets_pairs failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = 120):
        try:
            data = self.generics.traded_tickers(pairs_days=pairs_days)
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def last_trade(self):
        try:
            db = get_sqlite_db(db_path=self.db_path)
            data = db.query.get_pairs_last_trade()
            return data
        except Exception as e:  # pragma: no cover
            msg = f"pairs_last_trade failed for netid {self.netid}!"
            return default_error(e, msg)
