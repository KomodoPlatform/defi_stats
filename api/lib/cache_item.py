#!/usr/bin/env python3
import time
from util.files import Files
from util.urls import Urls
from util.exceptions import CacheFilenameNotFound
from util.logger import logger
from util.defaults import default_error, set_params, default_result


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
            self.update_data()
        except Exception as e:
            logger.error(f"Failed to init CacheItem '{name}': {e}")

    @property
    def data(self):
        return self._data

    def get_data(self):
        data = self.files.load_jsonfile(self.filename)
        if "last_updated" in data:
            since_updated = int(time.time()) - data["last_updated"]
            since_updated_min = int(since_updated / 60)
            if since_updated_min > 600:
                msg = f"{self.name} has not been updated for over {since_updated_min} minutes"
                logger.warning(msg)
        if "data" in data:
            return data["data"]
        return data

    def update_data(self):
        self._data = self.get_data()  # pragma: no cover

    @property
    def cache_expiry(self):
        expiry_limits = {"coins": 1440, "coins_config": 1440, "generic_last_traded": 1}
        if self.name in expiry_limits:
            return expiry_limits[self.name]
        return 5

    def save(self, data=None):  # pragma: no cover
        try:
            # Handle external mirrored data easily
            if self.source_url is not None:
                data = self.files.download_json(self.source_url)

            if data is not None:
                if len(data) > 0:
                    data = {"last_updated": int(time.time()), "data": data}
                    self.files.save_json(self.filename, data)
        except Exception as e:
            return default_error(e)
        msg = f"{self.filename} saved."
        return default_result(msg, loglevel="merge")
