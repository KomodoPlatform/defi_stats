#!/usr/bin/env python3
import time
from lib.external import FixerAPI, CoinGeckoAPI
from lib.generic import Generic
from lib.markets import Markets
from util.defaults import default_error, set_params, default_result
from util.exceptions import CacheFilenameNotFound, CacheItemNotFound
from util.files import Files
from util.logger import logger
from util.urls import Urls
from util.validate import validate_loop_data
from const import MARKETS_PAIRS_DAYS


class Cache:  # pragma: no cover
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Cache: {e}")

    def get_item(self, name):
        try:
            return CacheItem(name, **self.kwargs)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Error in [Cache.load_cache]: {e}"
            raise CacheItemNotFound(msg)

    def updated_since(self, healthcheck=False):
        updated = {}
        for i in [
            "coins_config",
            "gecko_source",
            "coins",
            "generic_pairs",
            "generic_last_traded",
            "fixer_rates",
            "prices_tickers_v1",
            "prices_tickers_v2",
        ]:
            item = self.get_item(i)
            since_updated = item.since_updated_min()
            if not healthcheck:
                logger.loop(f"[{i}] last updated: {since_updated} min")
            updated.update({i: since_updated})
        return updated


class CacheItem:
    def __init__(self, name, **kwargs) -> None:
        try:
            self.name = name
            self.kwargs = kwargs
            self.options = ["testing", "source_url", "netid"]
            set_params(self, self.kwargs, self.options)

            self.files = Files(testing=self.testing, netid=self.netid)
            self.filename = self.files.get_cache_fn(name)
            if self.filename is None:
                raise CacheFilenameNotFound(
                    f"Unable to find cache filename for '{name}'. Does it exist?"
                )

            self.urls = Urls()
            self.source_url = self.urls.get_cache_url(name)
            self._data = {}
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init CacheItem '{name}': {e}")

    @property
    def data(self):
        if len(self._data) is None:
            self.update_data()
        elif len(self._data) == 0:
            self.update_data()
        return self._data

    def get_data(self):
        data = self.files.load_jsonfile(self.filename)
        if "last_updated" in data:
            since_updated = int(time.time()) - data["last_updated"]
            since_updated_min = int(since_updated / 60)
            if since_updated_min > self.cache_expiry:
                msg = f"{self.name} has not been updated for over {since_updated_min} minutes"
                logger.warning(msg)
        if "data" in data:
            return data["data"]
        return data

    def since_updated_min(self):
        data = self.files.load_jsonfile(self.filename)
        if "last_updated" in data:
            since_updated = int(time.time()) - data["last_updated"]
            return int(since_updated / 60)
        return "unknown"

    def update_data(self):
        self._data = self.get_data()  # pragma: no cover

    @property
    def cache_expiry(self):
        expiry_limits = {
            "coins": 1440,
            "coins_config": 1440,
            "generic_last_traded": 1,
            "gecko_source": 15,
            "fixer_rates": 15,
        }
        if self.name in expiry_limits:
            return expiry_limits[self.name]
        return 5

    def save(self, data=None):  # pragma: no cover
        try:
            # Handle external mirrored data easily
            if self.source_url is not None:
                data = self.files.download_json(self.source_url)
            else:
                if self.name == "fixer_rates":
                    data = FixerAPI().latest()

                if self.name == "gecko_source":
                    data = CoinGeckoAPI().get_gecko_source()
                if self.name == "gecko_tickers":
                    data = Generic(netid="ALL").traded_tickers(pairs_days=7)

                if self.name == "generic_tickers":
                    data = Generic(netid="ALL").traded_tickers()
                if self.name == "generic_last_traded":
                    data = Generic(netid="ALL").last_traded()
                if self.name == "generic_pairs":
                    data = Generic(netid="ALL").traded_pairs_info()

                if self.name == "markets_pairs":
                    data = Markets(netid=self.netid).pairs(days=MARKETS_PAIRS_DAYS)
                if self.name == "markets_tickers":
                    data = Markets(netid=self.netid).tickers(
                        pairs_days=MARKETS_PAIRS_DAYS
                    )
            if data is not None:
                if validate_loop_data(data, self, "ALL"):
                    data = {"last_updated": int(time.time()), "data": data}
                    self.files.save_json(self.filename, data)
                else:
                    msg = f"failed to save {self.name}, data failed validation"
                    logger.warning(msg)
            else:
                msg = f"failed to save {self.name}, data is 'None'"
                logger.warning(msg)
        except Exception as e:  # pragma: no cover
            return default_error(e)
        msg = f"{self.filename} saved."
        return default_result(msg, loglevel="merge")


def load_gecko_source(testing=False):
    try:
        # logger.merge("Loading Gecko source")
        return CacheItem("gecko_source", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_gecko_source]: {e}")
        return {}


def load_coins_config(testing=False):
    try:
        return CacheItem("coins_config", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_coins_config]: {e}")
        return {}


def load_coins(testing=False):  # pragma: no cover
    try:
        return CacheItem("coins", testing=testing).data
    except Exception as e:
        logger.error(f"{type(e)} Error in [load_coins]: {e}")
        return {}


def load_generic_last_traded(testing=False):
    try:
        cache_item = CacheItem("generic_last_traded", testing=testing)
        return cache_item.data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [load_generic_last_traded]: {e}")
        return {}


def load_generic_pairs(testing=False):
    try:
        return CacheItem("generic_pairs", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [generic_pairs]: {e}")
        return {}


def load_generic_tickers(testing=False):
    try:
        return CacheItem("generic_tickers", testing=testing).data
    except Exception as e:  # pragma: no cover
        logger.error(f"{type(e)} Error in [generic_tickers]: {e}")
        return {}
