#!/usr/bin/env python3
import time
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List, Dict

from db.sqldb import SqlQuery
from db.schema import DefiSwap
from lib.cache import Cache
from lib.pair import Pair
from util.enums import TradeType
from models.generic import ErrorMessage, CoinTradeVolumes, PairTradeVolumes
from util.exceptions import UuidNotFoundException, BadPairFormatError
from util.logger import logger
import util.validate as validate


router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/distinct/",
    description="Get Unique values for a column",
    responses={406: {"model": ErrorMessage}},
    response_model=List,
    status_code=200,
)
def distinct(
    start_time: int = 0,
    end_time: int = 0,
    column: SqlQuery(with_enums=True).DefiSwapColumnsDistinct | None = None,
    coin: SqlQuery(with_enums=True).ValidCoins | None = None,
    pair: SqlQuery(with_enums=True).ValidPairs | None = None,
    pubkey: SqlQuery(with_enums=True).ValidPubkeys | None = None,
    gui: SqlQuery(with_enums=True).ValidGuis | None = None,
    version: SqlQuery(with_enums=True).ValidVersions | None = None,
    success_only: bool = True,
    failed_only: bool = False,
):
    try:
        if end_time == 0:
            end_time = int(time.time())
        query = SqlQuery()
        resp = sorted(
            query.get_distinct(
                start_time=start_time,
                end_time=end_time,
                column=column,
                coin=coin,
                pair=pair,
                gui=gui,
                version=version,
                failed_only=failed_only,
                success_only=success_only,
                pubkey=pubkey,
            )
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/get_swaps/",
    description="Swaps completed within two epoch timestamps.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[DefiSwap] | Dict[str, List[DefiSwap]] | Dict,
    status_code=200,
)
def get_swaps(
    start_time: int = 0,
    end_time: int = 0,
    coin: str | None = None,
    pair: str | None = None,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
    success_only: bool = True,
    failed_only: bool = False,
):
    try:
        if start_time == 0:
            start_time = int(time.time()) - 86400
        if end_time == 0:
            end_time = int(time.time())
        query = SqlQuery()
        resp = query.get_swaps(
            start_time=start_time,
            end_time=end_time,
            coin=coin,
            pair=pair,
            gui=gui,
            version=version,
            failed_only=failed_only,
            success_only=success_only,
            pubkey=pubkey,
        )
        return resp
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/historical_trades/{ticker_id}",
    description="Trade history for CoinGecko compatible pairs. Use format `KMD_LTC`",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def historical_trades(
    trade_type: TradeType = TradeType.ALL,
    ticker_id: str = "KMD_LTC",
    limit: int = 100,
    start_time: int = 0,
    end_time: int = 0,
    variant="ALL",
):
    try:
        if start_time == 0:
            start_time = int(time.time()) - 86400
        if end_time == 0:
            end_time = int(time.time())
        for value, name in [
            (limit, "limit"),
            (start_time, "start_time"),
            (end_time, "end_time"),
        ]:
            validate.positive_numeric(value, name, True)
        if start_time > end_time:
            raise ValueError("start_time must be less than end_time")
        if trade_type not in ["all", "buy", "sell"]:
            raise ValueError("trade_type must be one of: 'all', 'buy', 'sell'")
        pair = Pair(pair_str=ticker_id)

        data = pair.historical_trades(
            trade_type=trade_type,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )

        if variant in data:
            return data[variant]
        return data
        # resp = transform.sort_dict_list(resp, "timestamp", True)
        return data
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap/{uuid}",
    description="Get swap info from a uuid, e.g. `82df2fc6-df0f-439a-a4d3-efb42a3c1db8`",
    responses={406: {"model": ErrorMessage}},
    response_model=DefiSwap,
    status_code=200,
)
def get_swap(uuid: str):
    try:
        logger.info(uuid)
        query = SqlQuery()
        resp = query.get_swap(uuid=uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap_uuids",
    description="Get swap uuids for a pair (e.g. `KMD_LTC`).",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swap_uuids(
    start_time: int = 0,
    end_time: int = 0,
    coin: str | None = None,
    pair: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(time.time()) - 86400
        if end_time == 0:
            end_time = int(time.time())
        query = SqlQuery()
        uuids = query.swap_uuids(
            start_time=start_time, end_time=end_time, coin=coin, pair=pair
        )
        if coin is not None:
            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        elif pair is not None:
            return {
                "pair": pair,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        return {
            "start_time": start_time,
            "end_time": end_time,
            "swap_count": len(uuids),
            "swap_uuids": uuids,
        }
    except BadPairFormatError as e:
        err = {"error": e.name, "message": e.msg}
        return JSONResponse(status_code=e.status_code, content=err)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/coin_trade_volumes_usd",
    description="Trade volumes for each coin over the selected time period.",
    responses={406: {"model": ErrorMessage}},
    response_model=CoinTradeVolumes,
    status_code=200,
)
def coin_trade_volumes_usd(
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    version: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(time.time()) - 86400
        if end_time == 0:
            end_time = int(time.time())
        query = SqlQuery()
        volumes = query.coin_trade_volumes(
            start_time=start_time,
            end_time=end_time,
        )
        return query.coin_trade_volumes_usd(volumes=volumes)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pair_trade_volumes_usd",
    description="Trade volumes for each pair over the selected time period.",
    responses={406: {"model": ErrorMessage}},
    response_model=PairTradeVolumes,
    status_code=200,
)
def pair_trade_volumes_usd(
    start_time: int = 0,
    end_time: int = 0,
    pubkey: str | None = None,
    gui: str | None = None,
    coin: str | None = None,
    version: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(time.time()) - 86400
        if end_time == 0:
            end_time = int(time.time())
        query = SqlQuery()
        volumes = query.pair_trade_volumes(
            start_time=start_time,
            end_time=end_time,
            version=version,
            pubkey=pubkey,
            coin=coin,
            gui=gui,
        )
        return query.pair_trade_volumes_usd(volumes=volumes)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
