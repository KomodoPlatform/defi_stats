#!/usr/bin/env python3
from datetime import datetime
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from const import NODE_TYPE
import db.sqldb as db
import db.sqlitedb_merge as old_db_merge
import util.defaults as default
import util.memcache as memcache
from lib.cache import Cache, CacheItem, reset_cache_files
from lib.cache_calc import CacheCalc
from util.logger import logger, timed

router = APIRouter()


@router.on_event("startup")
@repeat_every(seconds=900)
@timed
def check_cache():  # pragma: no cover
    """Checks when cache items last updated"""
    try:
        cache = Cache()
        cache.healthcheck(to_console=True)
        memcache_stats = memcache.stats()
        for k, v in memcache_stats.items():
            # https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L1270-L1421
            if (
                k.decode("UTF-8")
                in [
                    "listen_disabled_num",
                    "uptime",
                    "curr_items",
                    "total_items",
                    "bytes",
                    "slab_reassign_evictions_nomem",
                    "get_hits",
                    "get_misses",
                    "get_expired",
                    "rejected_connections",
                    "connection_structures",
                    "max_connections",
                    "curr_connections",
                    "total_connections",
                    "limit_maxbytes",
                    "accepting_conns",
                    "evictions",
                    "reclaimed",
                    "response_obj_oom",
                    "response_obj_count" "read_buf_count",
                    "read_buf_bytes",
                    "read_buf_bytes_free",
                ]
                or 1 == 1
            ):
                default.memcache_stat(
                    msg=f"{k.decode('UTF-8'):<30}: {v}", loglevel="cached"
                )
        # "listen_disabled_num"
    except Exception as e:
        return default.result(msg=e, loglevel="warning")


@router.on_event("startup")
@timed
def init_missing_cache():  # pragma: no cover
    reset_cache_files()
    msg = "init missing cache loop complete!"
    return default.result(msg=msg, loglevel="loop", ignore_until=3)


# ORDERBOOKS CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def update_pairs_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            # builds from variant caches and saves to file
            CacheItem(name="pairs_orderbook_extended").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pairs_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def refresh_pairs_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            # threaded to populate variant caches
            CacheCalc().pairs_orderbook_extended(refresh=True)
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pairs_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# PRICES CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def refresh_prices_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_prices_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_prices_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# VOLUMES CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_pair_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# VOLUMES CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_pair_volumes_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_volumes_14d").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_volumes_14d refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_coin_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="coin_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "coin_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# LAST TRADE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def pairs_last_traded():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pairs_last_traded").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pairs_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def pairs_last_traded_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pairs_last_traded_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pairs_last_traded_24hr loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# MARKETS CACHE
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_markets_summary():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="markets_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "markets_summary loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def prices_service():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["prices_tickers_v1", "prices_tickers_v2"]:
                CacheItem(i).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Prices update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# EXTERNAL SOURCES CACHE
@router.on_event("startup")
@repeat_every(seconds=14400)
@timed
def coins():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["coins_config", "coins"]:
                CacheItem(i).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        return default.result(
            msg="Coins update loop complete!", loglevel="loop", ignore_until=5
        )


@router.on_event("startup")
@repeat_every(seconds=450)
@timed
def gecko_data():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_source").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Gecko source data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def gecko_pairs():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_pairs").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Gecko pairs data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def stats_api_summary():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("stats_api_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "stats_api_summary data update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def fixer_rates():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem(name="fixer_rates").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Fixer rates update loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# DATABASE SYNC
@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    if memcache.get("testing") is None:
        # updates last 24 hours swaps
        day = datetime.today().date()
        db.SqlSource().import_swaps_for_day(day)


@router.on_event("startup")
@repeat_every(seconds=180)
@timed
def import_dbs():
    if memcache.get("testing") is None:
        if NODE_TYPE != "serve":
            try:
                merge = old_db_merge.SqliteMerge()
                merge.import_source_databases()
            except Exception as e:
                return default.result(msg=e, loglevel="warning")
            msg = "Import source databases loop complete!"
            return default.result(msg=msg, loglevel="merge")
        msg = "Import source databases skipped, NodeType is 'serve'!"
        msg += " Masters will be updated from cron."
        return default.result(msg=msg, loglevel="merge")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def adex_fortnite():
    if memcache.get("testing") is None:
        try:
            pass
            CacheItem(name="adex_fortnite").save()
        except Exception as e:
            logger.warning(default.result(msg=e, loglevel="warning"))
        msg = "Adex fortnight loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def adex_24hr():
    if memcache.get("testing") is None:
        try:
            pass
            CacheItem(name="adex_24hr").save()
        except Exception as e:
            logger.warning(default.result(msg=e, loglevel="warning"))
        msg = "Adex 24hr loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# TICKERS
@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def pair_tickers():
    if memcache.get("testing") is None:
        try:
            CacheCalc().tickers(refresh=True)
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pairs_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


# REVIEW
@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def refresh_tickers():
    if memcache.get("testing") is None:
        try:
            CacheCalc().tickers()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Tickers refresh loop complete!"
        return default.result(msg=msg, loglevel="loop", ignore_until=0)


"""

@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def refresh_generic_tickers_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers_14d").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers 14d refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=90)
@timed
def get_generic_tickers_cache_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(
                name="generic_tickers_14d",
                from_memcache=True
            ).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers 14d loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_summary():
    if memcache.get("testing") is None:
        try:
            pass
            # CacheItem(name="generic_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Summary loop complete!"
        return default.result(msg=msg, loglevel="loop")
"""
