#!/usr/bin/env python3
from util.cron import cron
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from typing import List
from lib.pair import Pair
from lib.cache_calc import CacheCalc
from models.generic import ErrorMessage
from models.gecko import (
    GeckoPairsItem,
    GeckoTickers,
    GeckoOrderbook,
    GeckoHistoricalTrades,
)
from util.enums import TradeType
from util.logger import logger
from util.transform import convert, deplatform, template, derive
import util.memcache as memcache
import util.transform as transform
import util.validate as validate


router = APIRouter()


# Gecko Endpoints
@router.get(
    "/pairs",
    description="A list of CoinGecko compatible pairs traded within the last week.",
    response_model=List[GeckoPairsItem],
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_pairs():
    try:
        coins = memcache.get_coins_config()
        cache = memcache.get_pair_last_traded()
        ts = cron.now_utc() - 86400 * 7
        pairs = derive.pairs_traded_since(ts=ts, pairs_last_trade_cache=cache)
        return [template.gecko_pair_item(i, coins) for i in pairs]
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/pairs]: {e}"}


@router.get(
    "/tickers",
    description="24-hour price & volume for each CoinGecko compatible pair traded in last 7 days.",
    response_model=GeckoTickers,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_tickers():
    try:
        data = memcache.get_pair_orderbook_extended()
        volumes = memcache.get_pair_volumes_24hr()
        prices_data = memcache.get_pair_prices_24hr()
        resp = {
            "last_update": int(cron.now_utc()),
            "pairs_count": data["pairs_count"],
            "swaps_count": data["swaps_24hr"],
            "combined_volume_usd": data["volume_usd_24hr"],
            "combined_liquidity_usd": data["combined_liquidity_usd"],
            "data": []
        }
        logger.calc(data.keys())
        for depair in data['orderbooks']:
            base, quote = derive.base_quote(depair)
            x = data['orderbooks'][depair]["ALL"]
            if depair in volumes["volumes"]:
                vols = volumes['volumes'][depair]["ALL"]
            else:
                vols = template.pair_trade_vol_item()
            if depair in prices_data:
                prices = prices_data[depair]["ALL"]
            else:
                prices = template.pair_prices_info(suffix="24hr", base=base, quote=quote)
            resp["data"].append(convert.pair_orderbook_extras_to_gecko_tickers( x, vols, prices))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    "/orderbook/{ticker_id}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    response_model=GeckoOrderbook,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_orderbook(
    response: Response,
    ticker_id: str = "KMD_LTC",
    depth: int = 100,
):
    try:
        pair = Pair(pair_str=ticker_id)
        data = pair.orderbook(pair_str=ticker_id, depth=depth, no_thread=True)
        data = transform.orderbook_to_gecko(data)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=GeckoHistoricalTrades,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_historical_trades(
    response: Response,
    trade_type: TradeType = TradeType.ALL,
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = int(cron.now_utc() - 86400),
    end_time: int = int(cron.now_utc())
):
    try:
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        pair = Pair(pair_str=ticker_id)
        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )['ALL']
        data["buy"] = [convert.historical_trades_to_gecko(i) for i in data["buy"]]
        data["sell"] = [convert.historical_trades_to_gecko(i) for i in data["sell"]]
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
