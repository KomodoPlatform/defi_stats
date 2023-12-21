#!/usr/bin/env python3
import time
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every
from logger import logger
from cache import Cache
from db import update_master_sqlite_dbs
from enums import NetId


router = APIRouter()
cache = Cache()


@router.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
    try:
        cache.save.save_coins()
        cache.save.save_coins_config()
        return {"result": "Updated coins"}
    except IOError as e:
        err = f"Error in [update_coins]: {e}"
        logger.warning(err)
        return {"error": err}


@router.on_event("startup")
@repeat_every(seconds=120)
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.save_gecko_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@router.on_event("startup")
@repeat_every(seconds=60)
def cache_prices_service():  # pragma: no cover
    try:
        cache.save.save_prices_tickers_v1()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_prices_service]: {e}")
    try:
        cache.save.save_prices_tickers_v2()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_prices_service]: {e}")


@router.on_event("startup")
@repeat_every(seconds=600)
def cache_fixer_rates():  # pragma: no cover
    try:
        cache.save.save_fixer_rates_source()
    except IOError as e:
        logger.warning(f"{type(e)} Error in [cache_fixer_rates]: {e}")


@router.on_event("startup")
@repeat_every(seconds=300)
def update_dbs():
    started = int(time.time())
    try:
        update_master_sqlite_dbs()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_dbs]: {e}")
    logger.stopwatch(f"Time to process [update_dbs]: {int(time.time()) - started} sec")


@router.on_event("startup")
@repeat_every(seconds=120)
def update_gecko_pairs():
    started = int(time.time())
    try:
        r = cache.save.save_gecko_pairs(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_gecko_pairs] netid 'all': {e}")
    logger.stopwatch(
        f"Time to process {r[1]} pairs in [update_gecko_pairs]: {int(time.time()) - started} sec"
    )


@router.on_event("startup")
@repeat_every(seconds=130)
def update_gecko_tickers():
    started = int(time.time())
    try:
        r = cache.save.save_gecko_tickers(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_gecko_tickers]: {e}")
    logger.stopwatch(
        f"Time to process {r[1]} pairs in [update_gecko_tickers]: {int(time.time()) - started} sec"
    )


@router.on_event("startup")
@repeat_every(seconds=65)
def update_markets_last_trade():
    # This one is fast, so can do all netids in seq in same func
    started = int(time.time())
    started = int(time.time())
    for netid in NetId:
        try:
            r = cache.save.save_markets_last_trade(netid=netid.value)
        except Exception as e:
            logger.warning(
                f"{type(e)} Error in [update_markets_last_trade] for {netid.value}: {e}"
            )
    msg = f"Time to process {r[1]} pairs in [update_markets_last_trade]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=125)
def update_markets_pairs_8762():
    started = int(time.time())
    try:
        r = cache.save.save_markets_pairs(netid=8762)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_pairs_8762]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_pairs_8762]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=120)
def update_markets_tickers_8762():
    started = int(time.time())
    try:
        r = cache.save.save_markets_tickers(netid=8762)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_tickers_8762]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_tickers_8762]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=120)
def update_markets_pairs_7777():
    started = int(time.time())
    try:
        r = cache.save.save_markets_pairs(netid=7777)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_pairs_7777]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_pairs_7777]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=120)
def update_markets_tickers_7777():
    started = int(time.time())
    try:
        r = cache.save.save_markets_tickers(netid=7777)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_tickers_7777]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_tickers_7777]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=120)
def update_markets_pairs_all():
    started = int(time.time())
    try:
        r = cache.save.save_markets_pairs(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_pairs_all]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_pairs_all]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)


@router.on_event("startup")
@repeat_every(seconds=120)
def update_markets_tickers_all():
    started = int(time.time())
    try:
        r = cache.save.save_markets_tickers(netid="all")
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_markets_tickers_all]: {e}")
    msg = f"Time to process {r[1]} pairs in [update_markets_tickers_all]: "
    msg += f"{int(time.time()) - started} sec"
    logger.stopwatch(msg)
