#!/usr/bin/env python3
from util.files import Files
from util.urls import Urls
from util.exceptions import CacheFilenameNotFound
from util.logger import logger
from util.defaults import default_error, set_params, default_result


class CacheItem:
    def __init__(self, name, **kwargs) -> None:
        try:
            self.kwargs = kwargs
            self.options = ["testing", "source_url", "netid"]
            set_params(self, self.kwargs, self.options)
            self.name = name
            self.files = Files(testing=self.testing, netid=self.netid)
            self.urls = Urls()
            self.filename = self.files.get_cache_fn(name)
            self.source_url = self.urls.get_cache_url(name)
            if self.filename is None:
                raise CacheFilenameNotFound(
                    f"Unable to find cache filename for '{name}'. Does it exist?"
                )
            self._data = self.files.load_jsonfile(self.filename)
        except Exception as e:
            logger.error(f"Failed to init CacheItem: {e}")

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self):
        self._data = self.files.load_jsonfile(self.filename)  # pragma: no cover

    def save(self, data=None):  # pragma: no cover
        try:
            # Handle external mirrored data easily
            if self.source_url is not None:
                data = self.files.download_json(self.source_url)

            if data is not None:
                if len(data) > 0:
                    self.files.save_json(self.filename, data)
        except Exception as e:
            return default_error(e)
        msg = f"{self.filename} saved."
        return default_result(msg, loglevel="merge")
