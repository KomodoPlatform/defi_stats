#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import time
from db.sqlitedb import get_sqlite_db
from models.generic import ErrorMessage, SwapUuids, SwapItem
from lib.pair import Pair
from util.exceptions import UuidNotFoundException, BadPairFormatError
from util.logger import logger
from util.enums import NetId
from util.validate import validate_pair

router = APIRouter()


@router.get(
    "/swap/{uuid}",
    description="Get swap info from a uuid.",
    responses={406: {"model": ErrorMessage}},
    response_model=SwapItem,
    status_code=200,
)
def get_swap(uuid: str, netid: NetId = NetId.ALL):
    try:
        db = get_sqlite_db(netid=netid.value)
        resp = db.query.get_swap(uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return db.query.get_swap(uuid)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap_uuids/{pair}",
    description="Get swap uuids for a pair (e.g. `KMD_LTC`).",
    responses={406: {"model": ErrorMessage}},
    response_model=SwapUuids,
    status_code=200,
)
def swap_uuids(
    pair: str = "KMD_LTC",
    start_time: int = int(time.time() - 86400),
    end_time: int = int(time.time()),
    netid: NetId = NetId.ALL,
):
    try:
        validate_pair(pair)
        pair = Pair(pair_str=pair, netid=netid.value)
        uuids = pair.swap_uuids(start_time=start_time, end_time=end_time)
        resp = {"pair": pair.as_str, "swap_count": len(uuids), "swap_uuids": uuids}
        logger.info(resp)
        return resp
    except BadPairFormatError as e:
        err = {"error": e.name, "message": e.msg}
        return JSONResponse(status_code=e.status_code, content=err)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
