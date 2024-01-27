#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from decimal import Decimal
from datetime import datetime, timedelta
import util.cron as cron
from const import MARKETS_PAIRS_DAYS
from db.sqlitedb import get_sqlite_db
from models.generic import ErrorMessage
from util.exceptions import BadPairFormatError
from lib.generic import Generic
from lib.markets import Markets
from util.enums import TradeType, NetId
from util.logger import logger
import util.validate as validate
import util.transform as transform
import lib

clean = transform.Clean()
router = APIRouter()



@router.get(
    "/atomicdexio",
    description="Returns atomic swap counts over a variety of periods",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def atomicdex_info_api(netid: NetId = NetId.ALL):
    # TODO: Move to new DB
    db = get_sqlite_db(netid=netid.value)
    data = db.query.swap_counts()
    data.update({"swaps_24h": data["swaps_24hr"]})
    del data["swaps_24hr"]
    return data


# New endpoint
@router.get(
    "/current_liquidity",
    description="Global liquidity on the orderbook for all pairs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def current_liquidity(netid: NetId = NetId.ALL):
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        return {"current_liquidity": data["combined_liquidity_usd"]}

    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/current_liquidity]: {e}"}


@router.get(
    "/fiat_rates",
    description="Coin prices in USD (where available)",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def fiat_rates():
    data = lib.CacheItem("gecko_source").data
    return data


@router.get(
    "/orderbook/{market_pair}",
    description="Get Orderbook for a market pair in `KMD_LTC` format.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def orderbook(market_pair: str = "KMD_LTC", netid: NetId = NetId.ALL, depth: int = 100):
    try:
        generic = Generic(netid=netid.value)
        return generic.orderbook(pair_str=market_pair, depth=depth)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pairs_last_trade",
    description="Returns last trade info for all pairs matching the filter",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def pairs_last_traded(
    netid: NetId = NetId.ALL,
    start_time: int = 0,
    end_time: int = int(cron.now_utc()),
    min_swaps: int = 5,
) -> list:
    data = lib.CacheItem("generic_last_traded", netid=netid.value).data
    filtered_data = []
    for i in data:
        if data[i]["swap_count"] > min_swaps:
            if data[i]["last_swap"] > start_time:
                if data[i]["last_swap"] < end_time:
                    data[i].update({"pair": i})
                    filtered_data.append(data[i])
    return filtered_data


# Migrated from https://stats.testchain.xyz/api/v1/summary
@router.get(
    "/summary",
    description=f"24-hour price & volume for each pair traded in last {MARKETS_PAIRS_DAYS} days.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary(netid: NetId = NetId.ALL):
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_xyz_summary(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/tickers]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/tickers]: {e}"}


# Migrated from https://stats.testchain.xyz/api/v1/summary_for_ticker/KMD
@router.get(
    "/summary_for_ticker/{coin}",
    description="24h price & volume for market pairs with a specific coin traded in last 7 days",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def summary_for_ticker(coin: str = "KMD", netid: NetId = NetId.ALL):
    # TODO: Segwit not merged in this endpoint yet
    try:
        if "_" in coin:
            return {"error": "Coin value '{coin}' looks like a pair."}
        cache = lib.Cache(netid=netid.value)
        last_traded = cache.get_item(name="generic_last_traded").data
        resp = cache.get_item(name="markets_tickers").data
        new_data = []
        for i in resp["data"]:
            if coin in [i["base_currency"], i["target_currency"]]:
                if i["last_trade"] == 0:
                    if i["ticker_id"] in last_traded:
                        i["last_trade"] = last_traded[i["ticker_id"]]["last_swap"]
                        i["last_price"] = last_traded[i["ticker_id"]]["last_swap"]

                new_data.append(transform.to_summary_for_ticker_xyz_item(i))

        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/summary_for_ticker]: {e}"}


@router.get(
    "/swaps24/{ticker}",
    description="Total swaps involving a specific ticker (e.g. `KMD`) in the last 24hrs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swaps24(ticker: str = "KMD", netid: NetId = NetId.ALL) -> dict:
    # TODO: Lower than xyz source. Is it combined?
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        trades = 0
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                trades += int(i["trades_24hr"])
        return {"ticker": ticker, "swaps_amount_24h": trades}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/ticker",
    description="Simple last price and liquidity for each market pair, traded in last 7 days.",
)
def ticker(netid: NetId = NetId.ALL):
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        resp = []
        for i in data["data"]:
            resp.append(transform.ticker_to_market_ticker(i))
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
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        resp = []
        for i in data["data"]:
            if ticker in [i["base_currency"], i["target_currency"]]:
                resp.append(transform.ticker_to_market_ticker(i))
        return resp
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/ticker_for_ticker]: {e}"}


@router.get(
    "/tickers_summary",
    description="Total swaps and volume involving for each active ticker in the last 24hrs.",
)
def tickers_summary(netid: NetId = NetId.ALL):
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
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
        resp = clean.decimal_dict(resp)
        with_action = {}
        tickers = list(resp.keys())
        tickers.sort()
        for ticker in tickers:
            if resp[ticker]["trades_24h"] > 0:
                with_action.update({ticker: resp[ticker]})
        return with_action
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/market/swaps24]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/market/swaps24]: {e}"}


@router.get(
    "/trades/{market_pair}/{days_in_past}",
    description="Trades for the last 'x' days for a pair in `KMD_LTC` format.",
)
def trades(
    market_pair: str = "KMD_LTC", days_in_past: int | None = None, all: str = "false"
):
    try:
        all = all.lower() == "true"
        for value, name in [(days_in_past, "days_in_past")]:
            validate.positive_numeric(value, name)
        if days_in_past > 7:
            return {"error": "Maximum value for 'days_in_past' is '7'. Try again."}
        data = Markets().trades(pair=market_pair, days_in_past=days_in_past, all=all)
        for i in data:
            data["base_volume"] = float(data["base_volume"])
            data["quote_volume"] = float(data["quote_volume"])
        return data
    except BadPairFormatError as e:
        err = {"error": f"{e.msg}"}
        logger.warning(err)
    except Exception as e:
        err = {"error": f"{type(e)}: {e}"}
        logger.warning(err)
    return JSONResponse(status_code=400, content=err)


# Migrated from https://stats.testchain.xyz/api/v1/usd_volume_24h
@router.get(
    "/usd_volume_24hr",
    description="Volume (in USD) traded in last 24hrs.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def usd_volume_24h(netid: NetId = NetId.ALL):
    try:
        cache = lib.Cache(netid=netid.value)
        data = cache.get_item(name="markets_tickers").data
        return {"usd_volume_24h": data["combined_volume_usd"]}
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}")
        return {"error": f"{type(e)} Error in [/api/v3/markets/usd_volume_24h]: {e}"}


# TODO: get volumes for x days for ticker
@router.get(
    "/volumes_ticker/{coin}/{days_in_past}",
    description="Daily coin volume (e.g. `KMD, KMD-BEP20, KMD-ALL`) traded last 'x' days.",
)
def volumes_history_ticker(
    coin="KMD",
    days_in_past=1,
    trade_type: TradeType = TradeType.ALL,
    netid: NetId = NetId.ALL,
):
    # TODO: Move to new DB
    db = get_sqlite_db(netid=netid.value)
    volumes_dict = {}
    for i in range(0, int(days_in_past)):
        d = datetime.today() - timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d")
        day_ts = int(int(d.strftime("%s")) / 86400) * 86400
        # TODO: Align with midnight
        start_time = int(day_ts)
        end_time = int(day_ts) + 86400
        data = db.query.get_volume_for_coin(
            coin=coin,
            trade_type=trade_type,
            start_time=start_time,
            end_time=end_time,
        )
        volumes_dict[d_str] = data["data"][coin]
    return volumes_dict
