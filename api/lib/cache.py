#!/usr/bin/env python3
import os
from util.exceptions import CacheFilenameNotFound, CacheItemNotFound
from util.files import Files
from util.logger import logger, timed
from util.urls import Urls
from util.cron import cron
import util.defaults as default
import util.memcache as memcache
import util.validate as validate
import lib.cache_calc as cache_calc
import lib.external as external


class Cache:  # pragma: no cover
    def __init__(self, coins_config=None, **kwargs):
        try:
            self.kwargs = kwargs
            self._coins_config = coins_config
            self.options = []
            default.params(self, self.kwargs, self.options)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init Cache: {e}")

    @property
    def coins_config(self):
        if self._coins_config is None:
            self._coins_config = memcache.get_coins_config()
        return self._coins_config

    def get_item(self, name):
        try:
            return CacheItem(name, **self.kwargs)
        except Exception as e:  # pragma: no cover
            msg = f"{type(e)} Error in [Cache.load_cache]: {e}"
            raise CacheItemNotFound(msg)

    def healthcheck(self, to_console=False):  # pragma: no cover
        try:
            updated = {}
            for i in [
                "adex_24hr",
                "adex_fortnite",
                "coins",
                "coins_config",
                "fixer_rates",
                "gecko_source",
                "gecko_pairs",
                "pairs_last_traded",
                "pair_volumes_24hr",
                "pair_volumes_14d",
                "coin_volumes_24hr",
                "pairs_orderbook_extended",
                "prices_tickers_v1",
                "prices_tickers_v2",
                "tickers",
            ]:
                item = self.get_item(i)
                since_updated = item.since_updated_min()
                updated.update({i: since_updated})
                if to_console:
                    self.print_cache_status(i, since_updated)
            return updated
        except Exception as e:  # pragma: no cover
            logger.warning(e)

    def print_cache_status(self, i, since_updated):
        msg = f"[{i}] last updated: {since_updated} min"
        return default.result(msg=msg, loglevel="cached")


class CacheItem:
    def __init__(
        self,
        name,
        from_memcache: bool = False,
        coins_config=None,
        gecko_source=None,
        **kwargs,
    ) -> None:
        try:
            self.name = name
            self.kwargs = kwargs
            self.from_memcache = from_memcache
            self._coins_config = coins_config
            self._gecko_source = gecko_source
            self.options = []
            self._data = {}
            default.params(self, self.kwargs, self.options)
            self.files = Files()
            self.filename = self.files.get_cache_fn(name)
            if self.filename is None:
                raise CacheFilenameNotFound(
                    f"Unable to find cache filename for '{name}'. Does it exist?"
                )

            self.urls = Urls()
            self.source_url = self.urls.get_cache_url(name)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init CacheItem '{name}': {e}")

    @property
    def coins_config(self):
        if self._coins_config is None:
            self._coins_config = memcache.get_coins_config()
        return self._coins_config

    @property
    def gecko_source(self):
        if self._gecko_source is None:
            logger.calc("sourcing gecko")
            self._gecko_source = memcache.get_gecko_source()
        return self._gecko_source

    @property
    def data(self):  # pragma: no cover
        if len(self._data) is None:
            self.update_data()
        elif len(self._data) == 0:
            self.update_data()
        return self._data

    def get_data(self):
        data = {}
        if self.filename is not None:
            data = self.files.load_jsonfile(self.filename)
            if data is not None:  # pragma: no cover
                if "last_updated" in data:
                    since_updated = int(cron.now_utc()) - data["last_updated"]
                    since_updated_min = int(since_updated / 60)
                    if since_updated_min > self.cache_expiry:
                        msg = f"{self.name} has not been updated for over {since_updated_min} min"
                        logger.muted(msg)
                if "data" in data:
                    return data["data"]
        return data

    def since_updated_min(self):  # pragma: no cover
        if self.filename is not None:
            data = self.files.load_jsonfile(self.filename)
            if data is not None:
                if "last_updated" in data:
                    since_updated = int(cron.now_utc()) - data["last_updated"]
                    return int(since_updated / 60)
        return "unknown"

    def update_data(self):
        self._data = self.get_data()  # pragma: no cover

    @property
    def cache_expiry(self):
        expiry_limits = {
            "adex_24hr": 5,
            "adex_fortnite": 10,
            "coins": 1440,
            "coins_config": 1440,
            "pairs_last_traded": 5,
            "gecko_source": 15,
            "markets_summary": 5,
            "fixer_rates": 15,
            "pair_volumes_24hr": 15,
            "pair_volumes_14d": 15,
            "coin_volumes_24hr": 15,
            "pairs_orderbook_extended": 15,
            "gecko_pairs": 5,
            "tickers": 5,
        }
        if self.name in expiry_limits:
            return expiry_limits[self.name]
        return 5

    # TODO: Cache orderbooks to file? Volumes / prices? Liquidity? Swaps?
    # The reason to do this is to reduce population times on restarts.
    @timed
    def save(self, data=None):  # pragma: no cover
        try:
            # EXTERNAL SOURCE CACHE
            if self.source_url is not None:
                data = self.files.download_json(self.source_url)
                if self.name == "coins_config":
                    memcache.set_coins_config(data)
                if self.name == "coins":
                    memcache.set_coins(data)
            else:
                # EXTERNAL SOURCE CACHE
                if self.name == "fixer_rates":
                    data = external.FixerAPI().latest()
                    memcache.set_fixer_rates(data)

                if self.name == "gecko_source":
                    data = external.CoinGeckoAPI(
                        coins_config=self.coins_config
                    ).get_gecko_source()
                    memcache.set_gecko_source(data)

                # FOUNDATIONAL CACHE
                if self.name == "adex_24hr":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).adex_24hr(refresh=True)
                    memcache.set_adex_24hr(data)

                if self.name == "adex_fortnite":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).adex_fortnite(refresh=True)
                    memcache.set_adex_fortnite(data)

                if self.name == "coin_volumes_24hr":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).coin_volumes_24hr()
                    memcache.set_coin_volumes_24hr(data)

                # PAIR Data
                if self.name == "pairs_last_traded_24hr":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pairs_last_traded(since=cron.days_ago(1))
                    memcache.set_pairs_last_traded_24hr(data)

                if self.name == "pairs_last_traded":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pairs_last_traded()
                    memcache.set_pairs_last_traded(data)

                if self.name == "pairs_orderbook_extended":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pairs_orderbook_extended()
                    memcache.set_pairs_orderbook_extended(data)

                if self.name == "pair_prices_24hr":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pair_prices_24hr()
                    memcache.set_pair_prices_24hr(data)

                if self.name == "pair_volumes_24hr":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pair_volumes_24hr()
                    memcache.set_pair_volumes_24hr(data)

                if self.name == "pair_volumes_14d":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).pair_volumes_14d()
                    memcache.set_pair_volumes_14d(data)

                # MARKETS
                if self.name == "markets_summary":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).markets_summary()
                    memcache.set_markets_summary(data)

                if self.name == "tickers":
                    data = cache_calc.CacheCalc(coins_config=self.coins_config).tickers(
                        refresh=True
                    )
                    memcache.set_tickers(data)

                if self.name == "gecko_pairs":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).gecko_pairs(refresh=True)
                    memcache.set_gecko_pairs(data)

                if self.name == "stats_api_summary":
                    data = cache_calc.CacheCalc(
                        coins_config=self.coins_config
                    ).stats_api_summary(refresh=True)
                    memcache.set_stats_api_summary(data)

                # REVIEW
                """
                if self.name == "generic_summary":
                    data = stats_api.StatsAPI().pair_summaries()
                    memcache.set_summary(data)

                if self.name == "generic_tickers_14d":
                    data = cache_calc.CacheCalc().tickers(trades_days=14)
                    memcache.set_tickers_14d(data)
                """

            if data is not None:
                if validate.loop_data(data, self):
                    # Save without extra fields for upstream cache
                    if self.name in ["prices_tickers_v2", "fixer_rates"]:
                        fn = self.filename.replace(".json", "_cache.json")
                        self.files.save_json(fn, data)
                    data = {"last_updated": int(cron.now_utc()), "data": data}
                    r = self.files.save_json(self.filename, data)
                    msg = f"Saved {self.filename}"
                    return default.result(
                        data=data,
                        msg=r["msg"],
                        loglevel=r["loglevel"],
                        ignore_until=r["ignore_until"],
                    )
                else:
                    logger.warning(
                        f"failed to save {self.name}, data failed validation: {data}"
                    )
            else:
                logger.warning(f"failed to save {self.name}, data is 'None'")

        except Exception as e:  # pragma: no cover
            msg = f"{self.filename} Failed. {type(e)}: {e}"
            return default.error(e, msg=msg)


def reset_cache_files():
    if 'IS_TESTING' in os.environ:
        logger.calc(f"Resetting cache [testing: {os.environ['IS_TESTING']}]")
    else:
        os.environ['IS_TESTING'] = "False"
    memcache.set_coins_config(CacheItem(name="coins_config").data)
    coins_config = memcache.get_coins_config()
    memcache.set_coins(CacheItem(name="coins", coins_config=coins_config).data)
    memcache.set_fixer_rates(
        CacheItem(name="fixer_rates", coins_config=coins_config).data
    )
    memcache.set_gecko_source(
        CacheItem(name="gecko_source", coins_config=coins_config).data
    )
    memcache.set_gecko_pairs(
        CacheItem(name="gecko_pairs", coins_config=coins_config).data
    )
    memcache.set_coin_volumes_24hr(
        CacheItem(name="coin_volumes_24hr", coins_config=coins_config).data
    )
    memcache.set_pairs_last_traded(
        CacheItem(name="pairs_last_traded", coins_config=coins_config).data
    )
    memcache.set_pair_prices_24hr(
        CacheItem(name="pair_prices_24hr", coins_config=coins_config).data
    )
    memcache.set_pair_volumes_24hr(
        CacheItem(name="pair_volumes_24hr", coins_config=coins_config).data
    )
    memcache.set_pair_volumes_14d(
        CacheItem(name="pair_volumes_14d", coins_config=coins_config).data
    )
    memcache.set_pairs_orderbook_extended(
        CacheItem(name="pairs_orderbook_extended", coins_config=coins_config).data
    )
    memcache.set_adex_24hr(CacheItem(name="adex_24hr", coins_config=coins_config).data)
    memcache.set_adex_fortnite(
        CacheItem(name="adex_fortnite", coins_config=coins_config).data
    )
    memcache.set_tickers(CacheItem(name="tickers", coins_config=coins_config).data)
    memcache.set_markets_summary(
        CacheItem(name="markets_summary", coins_config=coins_config).data
    )
    memcache.set_stats_api_summary(
        CacheItem(name="stats_api_summary", coins_config=coins_config).data
    )
