#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
import time
from typing import List
from util.logger import logger
from lib.models import (
    UsdVolume,
    CurrentLiquidity,
    Swaps24,
    PairTrades,
    AdexIo,
)
from lib.cache import Cache
from lib.cache import CacheItem
from lib.pair import Pair
from util.enums import TradeType, NetId
from util.transform import (
    ticker_to_market_ticker,
    ticker_to_market_ticker_summary,
    sort_dict_list,
)
from util.validate import validate_ticker_id
from db.sqlitedb import get_sqlite_db
from util.transform import clean_decimal_dict
from lib.generics import Generics


router = APIRouter()
cache = Cache()


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    "/usd_volume_24h",
    response_model=UsdVolume,
    description="24-hour price & volume for each market pair traded in last 7 days.",
)
def usd_volume_24h(netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        return {"usd_volume_24h": data["combined_volume_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}


# New endpoint
@router.get(
    "/current_liquidity",
    response_model=CurrentLiquidity,
    description="24-hour price & volume for each market pair traded in last 7 days.",
)
def current_liquidity(netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        return {"current_liquidity": data["combined_liquidity_usd"]}

    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    "/summary",
    description="24-hour price & volume for each market pair traded in last 120 days.",
)
def summary(netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            resp.append(ticker_to_market_ticker_summary(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/tickers]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    "/summary_for_ticker/{ticker}",
    description="24h price & volume for market pairs with a specific ticker traded in last 7 days",
)
def summary_for_ticker(ticker: str, netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        last_trade_cache = cache.load_markets_last_trade(netid=netid.value)
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                ignore = [
                    "base_currency",
                    "target_currency",
                    "target_usd_price",
                    "base_usd_price",
                    "ticker_id",
                    "pool_id",
                ]
                if "KMD_LTC" == i["ticker_id"]:
                    logger.muted(i)
                if sum([Decimal(v) for k, v in i.items() if k not in ignore]) > 0:
                    item = ticker_to_market_ticker_summary(i)
                    if i["ticker_id"] in last_trade_cache:
                        x = last_trade_cache[i["ticker_id"]]["last_taker_amount"]
                        y = last_trade_cache[i["ticker_id"]]["last_maker_amount"]
                        z = last_trade_cache[i["ticker_id"]]["last_swap_time"]
                        last_price = Decimal(x) / Decimal(y)
                        item.update(
                            {
                                "last_price": str(last_price),
                                "last_swap_timestamp": z,
                            }
                        )
                    resp.append(item)
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker(netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            resp.append(ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker]: {e}"}


@router.get(
    "/ticker_for_ticker",
    description="Simple last price and liquidity for each market pair for a specific ticker.",
)
def ticker_for_ticker(ticker, netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                resp.append(ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}"}


@router.get(
    "/swaps24/{ticker}",
    response_model=Swaps24,
    description="Total swaps involving a specific ticker in the last 24hrs.",
)
def swaps24(ticker, netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        trades = 0
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                trades += int(i["trades_24hr"])
        return {"ticker": ticker, "swaps_amount_24h": trades}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/orderbook/{market_pair}",
    description="Get Orderbook for a market pair in `KMD_LTC` format.",
    status_code=200,
)
def orderbook(market_pair="KMD_LTC", netid: NetId = NetId.ALL):
    try:
        generics = Generics(netid=netid.value)
        return generics.get_orderbook(market_pair, netid)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/trades/{market_pair}/{days_in_past}",
    response_model=List[PairTrades],
    description="Summary of trades for the last 'x' days.",
)
def trades(market_pair: str = "KMD_LTC", days_in_past=1, netid: NetId = NetId.ALL):
    market_pairs = cache.load_markets_pairs(netid=netid.value)
    valid_tickers = [ticker["ticker_id"] for ticker in market_pairs]
    ticker_type = validate_ticker_id(
        market_pair, valid_tickers, allow_reverse=True, allow_fail=True
    )
    resp = []
    if ticker_type == "reversed":
        reverse = True
    elif ticker_type == "failed":
        return resp
    else:
        reverse = False
    try:
        if netid.value == "all":
            for x in NetId:
                if x.value != "all":
                    pair = Pair(pair_str=market_pair, netid=x.value)
                    data = pair.historical_trades(
                        trade_type="all",
                        start_time=int(time.time() - 86400),
                        end_time=int(time.time()),
                        reverse=reverse,
                    )
                    resp += data["buy"]
                    resp += data["sell"]
        else:
            pair = Pair(pair_str=market_pair, netid=netid.value)
            data = pair.historical_trades(
                trade_type="all",
                start_time=int(time.time() - 86400),
                end_time=int(time.time()),
                reverse=reverse,
            )
            resp += data["buy"]
            resp += data["sell"]

        sorted_trades = sort_dict_list(resp, "timestamp", reverse=True)
        return sorted_trades
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/atomicdexio",
    description="Returns swap counts over a variety of periods",
    response_model=AdexIo,
)
def atomicdex_info_api(netid: NetId = NetId.ALL):
    db = get_sqlite_db(netid=netid.value)
    return db.swap_counts()


@router.get("/pairs_last_trade", description="Returns the last trade for all pairs")
def pairs_last_trade(
    netid: NetId = NetId.ALL, start_time=0, end_time=int(time.time()), min_swaps=5
):
    data = CacheItem("pairs_last_trade", netid=netid.value).data
    filtered_data = [
        i
        for i in data
        if i["last_swap_time"] > start_time and i["last_swap_time"] < end_time
    ]
    return filtered_data


# TODO: get volumes for x days for ticker
@router.get(
    "/volumes_ticker/{ticker}/{days_in_past}",
    description="Daily volume of a ticker traded for the last 'x' days.",
)
def volumes_history_ticker(
    ticker="KMD",
    days_in_past=1,
    trade_type: TradeType = TradeType.ALL,
    netid: NetId = NetId.ALL,
):
    db = get_sqlite_db(netid=netid.value)
    volumes_dict = {}
    for i in range(0, int(days_in_past)):
        db = get_sqlite_db(netid=netid.value)
        d = datetime.today() - timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d")
        day_ts = int(int(d.strftime("%s")) / 86400) * 86400
        # TODO: Align with midnight
        start_time = int(day_ts) - 86400
        end_time = int(day_ts)
        volumes_dict[d_str] = db.get_volume_for_ticker(
            ticker=ticker,
            trade_type=trade_type.value,
            start_time=start_time,
            end_time=end_time,
        )
        return volumes_dict


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary(netid: NetId = NetId.ALL):
    try:
        data = cache.load_markets_tickers(netid=netid.value)
        resp = {}
        for i in data["data"]:
            base = i["base_currency"]
            rel = i["target_currency"]
            for ticker in [base, rel]:
                if ticker not in resp:
                    resp.update({ticker: {"trades_24h": 0, "volume_24h": 0}})
                resp[ticker]["trades_24h"] += int(i["trades_24hr"])
                if ticker == base:
                    resp[ticker]["volume_24h"] += Decimal(i["base_volume"])
                elif ticker == rel:
                    resp[ticker]["volume_24h"] += Decimal(i["target_volume"])
        resp = clean_decimal_dict(resp)
        with_action = {}
        tickers = list(resp.keys())
        tickers.sort()
        for ticker in tickers:
            if resp[ticker]["trades_24h"] > 0:
                with_action.update({ticker: resp[ticker]})
        return with_action
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps_by_ticker_24h]: {e}")
        return {
            "error": f"{type(e)} Error in [/api/v3/market/swaps_by_ticker_24h]: {e}"
        }


@router.get(
    "/fiat_rates",
    description="Coin prices in USD (where available)",
)
def fiat_rates():
    data = CacheItem("gecko_source").data
    return data
