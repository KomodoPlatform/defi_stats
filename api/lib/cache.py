#!/usr/bin/env python3
from util.exceptions import CacheItemNotFound
from lib.cache_item import CacheItem
from util.defaults import set_params
from util.logger import logger


class Cache:  # pragma: no cover
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
        except Exception as e:
            logger.error(f"Failed to init Cache: {e}")

    def get_item(self, name):
        try:
            return CacheItem(name, **self.kwargs)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Error in [Cache.load_cache]: {e}"
            raise CacheItemNotFound(msg)
