#!/usr/bin/env python3
from db.sqlitedb import get_sqlite_db_paths
from lib.generic import Generic
from lib.external import CoinGeckoAPI
from util.files import Files
from util.logger import timed, logger
from util.defaults import default_error, set_params
from const import MARKETS_PAIRS_DAYS
import lib


class Markets:
    def __init__(self, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
            self.db_path = get_sqlite_db_paths(netid=self.netid)
            self.files = Files(netid=self.netid, testing=self.testing)
            self.gecko = CoinGeckoAPI(testing=self.testing)
            self.generic = Generic(**kwargs)
            self.last_traded = lib.load_generic_last_traded()
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Markets: {e}")

    @timed
    def pairs(self, days=MARKETS_PAIRS_DAYS):
        try:
            # Include unpriced, traded in last 30 days
            data = lib.load_generic_pairs()
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets.pairs failed for netid {self.netid}!"
            return default_error(e, msg)

    @timed
    def tickers(self, trades_days: int = 1, pairs_days: int = MARKETS_PAIRS_DAYS):
        try:
            data = self.generic.traded_tickers(pairs_days=pairs_days)
            return data
        except Exception as e:  # pragma: no cover
            msg = f"markets_tickers failed for netid {self.netid}!"
            return default_error(e, msg)
