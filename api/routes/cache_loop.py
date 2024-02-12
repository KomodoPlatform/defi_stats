#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from const import NODE_TYPE
import db.sqldb as db
import db.sqlitedb_merge as old_db_merge
import util.defaults as default
import util.memcache as memcache
from lib.cache import Cache, CacheItem
from lib.cache_calc import CacheCalc
from lib.coins import Coins
from util.logger import logger, timed

router = APIRouter()


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def check_cache():  # pragma: no cover
    """Checks when cache items last updated"""
    try:
        cache = Cache()
        cache.healthcheck(to_console=True)
        logger.info(memcache.stats())
    except Exception as e:
        return default.result(msg=e, loglevel="warning")


@router.on_event("startup")
@repeat_every(seconds=15)
@timed
def init_missing_cache():  # pragma: no cover
    memcache.set_coins(CacheItem(name="coins").data)
    memcache.set_coins_config(CacheItem(name="coins_config").data)
    memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
    memcache.set_gecko_source(CacheItem(name="gecko_source").data)
    # memcache.set_adex_fortnite(CacheItem(name="adex_fortnite").data)
    memcache.set_markets_summary(CacheItem(name="markets_summary").data)
    memcache.set_pair_last_traded(CacheItem(name="pair_last_traded").data)
    memcache.set_pair_volumes_24hr(CacheItem(name="pair_volumes_24hr").data)
    memcache.set_coin_volumes_24hr(CacheItem(name="coin_volumes_24hr").data)
    memcache.set_pair_orderbook_extended(CacheItem(name="pair_orderbook_extended").data)

    # memcache.set_summary(CacheItem(name="generic_summary").data)
    # memcache.set_tickers(CacheItem(name="generic_tickers").data)
    # memcache.set_tickers_14d(CacheItem(name="generic_tickers_14d").data)
    memcache.update("coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400)

    msg = "init missing cache loop complete!"
    return default.result(msg=msg, loglevel="loop", ignore_until=3)


# ORDERBOOKS CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def refresh_pair_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_orderbook_extended").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def refresh_pair_orderbook_extended():
    if memcache.get("testing") is None:
        try:
            CacheCalc().pair_orderbook_extended(no_thread=False)
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_orderbook_extended refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


# PRICES CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def refresh_prices_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_prices_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_prices_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


# VOLUMES CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_pair_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_coin_volumes_24hr():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="coin_volumes_24hr").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "coin_volumes_24hr refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


# LAST TRADE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def pair_last_traded():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="pair_last_traded").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "pair_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop")


# MARKETS CACHE
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_markets_summary():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="markets_summary").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "markets_summary loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def prices_service():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["prices_tickers_v1", "prices_tickers_v2"]:
                CacheItem(i).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Prices update loop complete!"
        return default.result(msg=msg, loglevel="loop")


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
        return default.result(msg="Coins update loop complete!", loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=600)
@timed
def gecko_data():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_source").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Gecko data update loop complete!"
        return default.result(msg=msg, loglevel="loop")


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
        return default.result(msg=msg, loglevel="loop")


# DATABASE SYNC
@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    if memcache.get("testing") is None:
        # updates last 24 hours swaps
        db.SqlSource().populate_pgsqldb()


@router.on_event("startup")
@repeat_every(seconds=150)
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
            # CacheItem(name="adex_fortnite").save()
        except Exception as e:
            logger.warning(default.result(msg=e, loglevel="warning"))
        msg = "Adex fortnight loop complete!"
        return default.result(msg=msg, loglevel="loop")


"""
# REVIEW
@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def refresh_generic_tickers():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers").save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers refresh loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def get_generic_tickers():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers", from_memcache=True).save()
        except Exception as e:
            return default.result(msg=e, loglevel="warning")
        msg = "Generic tickers loop for memcache complete!"
        return default.result(msg=msg, loglevel="loop")


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
@repeat_every(seconds=60)
@timed
def get_generic_tickers_cache_14d():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers_14d", from_memcache=True).save()
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
