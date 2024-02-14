#!/usr/bin/env python3
from util.cron import cron
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from typing import List
from lib.pair import Pair
from lib.cache_calc import CacheCalc
import lib.dex_api as dex
from models.generic import ErrorMessage
from models.gecko import (
    GeckoPairsItem,
    GeckoTickers,
    GeckoOrderbook,
    GeckoHistoricalTrades,
)
from util.enums import TradeType
from util.logger import logger
from util.transform import convert, deplatform, template, derive, invert, format_10f
import util.memcache as memcache
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
            "data": [],
        }
        logger.calc(data.keys())
        for depair in data["orderbooks"]:
            base, quote = derive.base_quote(depair)
            x = data["orderbooks"][depair]["ALL"]
            if depair in volumes["volumes"]:
                vols = volumes["volumes"][depair]["ALL"]
            else:
                vols = template.pair_trade_vol_item()
            if depair in prices_data:
                prices = prices_data[depair]["ALL"]
            else:
                prices = template.pair_prices_info(
                    suffix="24hr", base=base, quote=quote
                )
            resp["data"].append(
                convert.pair_orderbook_extras_to_gecko_tickers(x, vols, prices)
            )
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/gecko/tickers]: {e}"}


@router.get(
    "/orderbook/{pair_str}",
    description="Returns live orderbook for a compatible pair (e.g. `KMD_LTC` ).",
    # response_model=GeckoOrderbook,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_orderbook(
    response: Response,
    pair_str: str = "KMD_LTC",
    depth: int = 100,
):
    # No extras needed, but cache combines variants.
    try:
        book = memcache.get_pair_orderbook_extended()

        depair = deplatform.pair(pair_str)
        if depair in book["orderbooks"]:
            data = book["orderbooks"][depair]["ALL"]
            return convert.orderbook_to_gecko(data, depth=depth)
        elif invert.pair(depair) in book["orderbooks"]:
            data = book["orderbooks"][invert.pair(depair)]["ALL"]
            return convert.orderbook_to_gecko(data, depth=depth, reverse=True)
        # Use direct method if no cache.
        variant_cache_name = f"orderbook_{pair_str}"
        coins_config = memcache.get_coins_config()
        gecko_source = memcache.get_gecko_source()
        base, quote = derive.base_quote(pair_str=pair_str)
        data = dex.get_orderbook(
            base=base,
            quote=quote,
            coins_config=coins_config,
            gecko_source=gecko_source,
            variant_cache_name=variant_cache_name,
            depth=depth,
            no_thread=True,
        )
        resp = {
            "ticker_id": pair_str,
            "timestamp": int(cron.now_utc()),
            "variants": [pair_str],
            "bids": [
                [format_10f(i["price"]), format_10f(i["volume"])] for i in data["bids"]
            ][:depth],
            "asks": [
                [format_10f(i["price"]), format_10f(i["volume"])] for i in data["asks"]
            ][:depth],
        }
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/historical_trades/{pair_str}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    response_model=GeckoHistoricalTrades,
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def gecko_historical_trades(
    response: Response,
    trade_type: TradeType = TradeType.ALL,
    pair_str: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc() - 86400)
        if end_time == 0:
            end_time = int(cron.now_utc())
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        pair = Pair(pair_str=pair_str)
        data = pair.historical_trades(
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )["ALL"]
        base, target = derive.base_quote(pair_str=pair_str)
        resp = {
            "ticker_id": pair_str,
            "base": base,
            "target": target,
            "start_time": str(start_time),
            "end_time": str(end_time),
            "limit": str(limit),
            "trades_count": data["trades_count"],
            "sum_base_volume_buys": data["sum_base_volume_buys"],
            "sum_target_volume_buys": data["sum_quote_volume_buys"],
            "sum_base_volume_sells": data["sum_base_volume_sells"],
            "sum_target_volume_sells": data["sum_quote_volume_sells"],
            "average_price": data["average_price"],
            "buy": [convert.historical_trades_to_gecko(i) for i in data["buy"]],
            "sell": [convert.historical_trades_to_gecko(i) for i in data["sell"]],
        }
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
