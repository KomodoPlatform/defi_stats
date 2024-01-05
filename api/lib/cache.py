#!/usr/bin/env python3
from util.files import Files
from util.exceptions import CacheItemNotFound
from lib.external import CoinGeckoAPI
from lib.markets import Markets
from lib.cache_item import CacheItem
from db.sqlitedb import get_sqlite_db
from util.defaults import set_params
from util.logger import logger


class Cache:  # pragma: no cover
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid", "exclude_unpriced", "include_all_kmd"]
            set_params(self, self.kwargs, self.options)
            self.db = get_sqlite_db(netid=self.netid)
            self.files = Files(netid=self.netid, testing=self.testing)
            self.gecko = CoinGeckoAPI(testing=self.testing)
            self.markets = Markets(**kwargs)
            self.kwargs = kwargs
        except Exception as e:
            logger.error(f"Failed to init Cache: {e}")

    def get_item(self, name):
        try:
            return CacheItem(name, **self.kwargs)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Error in [Cache.load_cache]: {e}"
            raise CacheItemNotFound(msg)
