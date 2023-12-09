#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
import time
from typing import List
from logger import logger
from models import (
    UsdVolume,
    CurrentLiquidity,
    Swaps24,
    PairTrades,
    AdexIo,
)
from helper import get_mm2_rpc_port, sort_dict_list
from cache import Cache
from pair import Pair
from utils import Utils
from orderbook import Orderbook
from enums import TradeType, NetId
from transform import (
    gecko_ticker_to_market_ticker,
    gecko_ticker_to_market_ticker_summary,
)
from validate import validate_ticker_id
from const import MM2_DB_PATHS, MM2_RPC_PORTS
from db import get_sqlite_db

router = APIRouter()
cache = Cache()


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    "/usd_volume_24h",
    response_model=UsdVolume,
    description="24-hour price & volume for each market pair traded in last 7 days.",
)
def usd_volume_24h(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
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
def current_liquidity(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        return {"current_liquidity": data["combined_liquidity_usd"]}

    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    "/summary",
    description="24-hour price & volume for each market pair traded in last 7 days.",
)
def summary(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            resp.append(gecko_ticker_to_market_ticker_summary(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/tickers]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    "/summary_for_ticker/{ticker}",
    description="24h price & volume for market pairs with a specific ticker traded in last 7 days",
)
def summary_for_ticker(ticker: str, netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                resp.append(gecko_ticker_to_market_ticker_summary(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker(netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            resp.append(gecko_ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker]: {e}"}


@router.get(
    "/ticker_for_ticker",
    description="Simple last price and liquidity for each market pair for a specific ticker.",
)
def ticker_for_ticker(ticker, netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                resp.append(gecko_ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}"}


@router.get(
    "/swaps24/{ticker}",
    response_model=Swaps24,
    description="Total swaps involving a specific ticker in the last 24hrs.",
)
def swaps24(ticker, netid: NetId = NetId.NETID_7777):
    try:
        data = cache.load.load_gecko_tickers(netid=netid.value)
        trades = 0
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                trades += int(i["trades_24hr"])
        return {"ticker": ticker, "swaps_amount_24h": trades}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get("/orderbook/{market_pair}")
def orderbook(market_pair="KMD_LTC", netid: NetId = NetId.NETID_7777):
    try:
        gecko_pairs = cache.load.load_gecko_pairs(netid=netid.value)
        valid_tickers = [ticker["ticker_id"] for ticker in gecko_pairs]
        validate_ticker_id(market_pair, valid_tickers)
        resp = {
            "market_pair": market_pair,
            "timestamp": f"{int(time.time())}",
            "asks": [],
            "bids": [],
        }
        if netid.value == "all":
            for x in NetId:
                if x.value != "all":
                    mm2_port = get_mm2_rpc_port(netid=x.value)
                    data = Orderbook(
                        pair=Pair(market_pair), mm2_port=mm2_port
                    ).for_pair(endpoint=True)
                    resp["asks"] += data["asks"]
                    resp["bids"] += data["bids"]
                    resp["liquidity_usd"] += data["liquidity_usd"]
                    resp["total_asks_base_vol"] += data["total_asks_base_vol"]
                    resp["total_bids_base_vol"] += data["total_bids_base_vol"]
                    resp["total_asks_quote_vol"] += data["total_asks_quote_vol"]
                    resp["total_bids_quote_vol"] += data["total_bids_quote_vol"]
                    resp["total_asks_base_usd"] += data["total_asks_base_usd"]
                    resp["total_bids_quote_usd"] += data["total_bids_quote_usd"]
            resp["bids"] = resp["bids"][::-1]
            resp["asks"] = resp["asks"][::-1]
        else:
            mm2_port = get_mm2_rpc_port(netid=netid.value)
            data = Orderbook(pair=Pair(market_pair), mm2_port=mm2_port).for_pair(
                endpoint=True
            )
            resp["asks"] += data["asks"]
            resp["bids"] += data["bids"]
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/trades/{market_pair}/{days_in_past}",
    response_model=List[PairTrades],
    description="Summary of trades for the last 'x' days.",
)
def trades(
    market_pair: str = "KMD_LTC", days_in_past=1, netid: NetId = NetId.NETID_7777
):
    try:
        if netid.value == "all":
            resp = []
            for x in NetId:
                if x.value != "all":
                    pair = Pair(
                        pair=market_pair,
                        path_to_db=MM2_DB_PATHS[x.value],
                        mm2_port=MM2_RPC_PORTS[x.value],
                    )
                    data = pair.historical_trades(
                        trade_type="all",
                        netid=netid.value,
                        start_time=int(time.time() - 86400),
                        end_time=int(time.time()),
                    )
                    resp += data["buy"]
                    resp += data["sell"]
        else:
            pair = Pair(
                pair=market_pair,
                path_to_db=MM2_DB_PATHS[netid.value],
                mm2_port=MM2_RPC_PORTS[netid.value],
            )
            data = pair.historical_trades(
                trade_type="all",
                netid=netid.value,
                start_time=int(time.time() - 86400),
                end_time=int(time.time()),
            )
            resp = data["buy"] + data["sell"]

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
def atomicdex_info_api(netid: NetId = NetId.NETID_7777):
    db = get_sqlite_db(netid=netid.value)
    return db.swap_counts()


# TODO: get volumes for x days for ticker
@router.get(
    "/volumes_ticker/{ticker}/{days_in_past}",
    description="Daily volume of a ticker traded for the last 'x' days.",
)
def volumes_history_ticker(
    ticker="KMD",
    days_in_past=1,
    trade_type: TradeType = TradeType.ALL,
    netid: NetId = NetId.NETID_7777,
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


@router.get("/fiat_rates")
def fiat_rates():
    utils = Utils()
    return utils.load_jsonfile(utils.files.gecko_source_file)


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary(netid: NetId = NetId.NETID_7777):
    try:
        utils = Utils()
        data = cache.load.load_gecko_tickers(netid=netid.value)
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
        resp = utils.clean_decimal_dict(resp)
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps_by_ticker_24h]: {e}")
        return {
            "error": f"{type(e)} Error in [/api/v3/market/swaps_by_ticker_24h]: {e}"
        }
