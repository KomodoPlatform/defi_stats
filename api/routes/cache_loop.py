#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from const import NODE_TYPE, RESET_TABLE
import db
import util.defaults as default
import util.memcache as memcache
from lib.cache import Cache, CacheItem
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
    except Exception as e:
        return default.error(e)


# Pure Upstream Data Sourcing


@router.on_event("startup")
@repeat_every(seconds=14400)
@timed
def coins():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            for i in ["coins_config", "coins"]:
                CacheItem(i).save()
        except Exception as e:
            return default.error(e)
        return default.result(msg="Coins update loop complete!", loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=600)
@timed
def gecko_data():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem("gecko_source").save()
        except Exception as e:
            return default.error(e)
        msg = "Gecko data update loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=15)
@timed
def init_missing_cache():  # pragma: no cover
    try:
        if memcache.get("coins_config") is None:
            memcache.set_coins_config(CacheItem(name="coins_config").data)
        if memcache.get("fixer_rates") is None:
            memcache.set_fixer_rates(CacheItem(name="fixer_rates").data)
        if memcache.get("gecko_source") is None:
            memcache.set_gecko_source(CacheItem(name="gecko_source").data)
        if memcache.get("generic_adex_fortnite") is None:
            memcache.set_adex_fortnite(CacheItem(name="generic_adex_fortnite").data)
        if memcache.get("generic_last_traded") is None:
            memcache.set_last_traded(CacheItem(name="generic_last_traded").data)
        if memcache.get("generic_summary") is None:
            memcache.set_summary(CacheItem(name="generic_summary").data)
        if memcache.get("generic_tickers") is None:
            memcache.set_tickers(CacheItem(name="generic_tickers").data)
        if memcache.get("coins_with_segwit") is None:
            memcache.update(
                "coins_with_segwit", [i.coin for i in Coins().with_segwit], 86400
            )

    except Exception as e:
        return default.error(e)
    msg = "Gecko data update loop complete!"
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
            return default.error(e)
        msg = "Prices update loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def fixer_rates():  # pragma: no cover
    if memcache.get("testing") is None:
        try:
            CacheItem(name="fixer_rates").save()
        except Exception as e:
            return default.error(e)
        msg = "Fixer rates update loop complete!"
        return default.result(msg=msg, loglevel="loop")


# Generic Loops
@router.on_event("startup")
@repeat_every(seconds=60)
@timed
def generic_last_traded():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_last_traded").save()
        except Exception as e:
            return default.error(e)
        msg = "generic_last_traded loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=180)
@timed
def generic_tickers():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_tickers").save()
        except Exception as e:
            return default.error(e)
        msg = "Generic tickers (ALL) loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=150)
@timed
def import_dbs():
    if memcache.get("testing") is None:
        if NODE_TYPE != "serve":
            try:
                merge = db.SqliteMerge()
                merge.import_source_databases()
            except Exception as e:
                return default.error(e)
            msg = "Import source databases loop complete!"
            return default.result(msg=msg, loglevel="merge")
        msg = "Import source databases skipped, NodeType is 'serve'!"
        msg += " Masters will be updated from cron."
        return default.result(msg=msg, loglevel="merge")


@router.on_event("startup")
@repeat_every(seconds=300)
@timed
def populate_pgsqldb_loop():
    if memcache.get("testing") is None:
        if RESET_TABLE:
            db.SqlSource().reset_defi_stats_table()
        # updates last 24 hours swaps
        db.SqlSource().populate_pgsqldb()


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_adex_fortnite():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_adex_fortnite").save()
        except Exception as e:
            logger.warning(default.error(e))
        msg = "Adex fortnight loop complete!"
        return default.result(msg=msg, loglevel="loop")


@router.on_event("startup")
@repeat_every(seconds=120)
@timed
def generic_summary():
    if memcache.get("testing") is None:
        try:
            CacheItem(name="generic_summary").save()
        except Exception as e:
            return default.error(e)
        msg = "Summary loop complete!"
        return default.result(msg=msg, loglevel="loop")
