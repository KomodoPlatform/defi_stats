#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import time
from const import MM2_DB_PATHS, MM2_RPC_PORTS
from logger import logger
from models import ErrorMessage, GeckoSwapItem, SwapUuids
from generics import UuidNotFoundException
from db import get_sqlite_db
from pair import Pair
from enums import NetId

router = APIRouter()


@router.get(
    "/swap/{uuid}",
    description="Get swap info from a uuid.",
    responses={406: {"model": ErrorMessage}},
    response_model=GeckoSwapItem,
    status_code=200,
)
def get_swap(
    uuid: str,
    netid: NetId = NetId.ALL
):
    try:
        db = get_sqlite_db(netid=netid.value)
        resp = db.get_swap(uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return db.get_swap(uuid)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)


@router.get(
    "/swap_uuids/{pair}",
    description="Get swap info from a uuid.",
    responses={406: {"model": ErrorMessage}},
    response_model=SwapUuids,
    status_code=200,
)
def swap_uuids(
    pair: str,
    start_time: int = int(time.time() - 86400),
    end_time: int = int(time.time()),
    netid: NetId = NetId.ALL
):
    try:
        pair = Pair(
            pair=pair,
            path_to_db=MM2_DB_PATHS[netid.value],
            mm2_port=MM2_RPC_PORTS[netid.value],
        )
        uuids = pair.swap_uuids(start_time=start_time, end_time=end_time)
        resp = {"pair": pair.as_str, "swap_count": len(uuids), "swap_uuids": uuids}
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=406, content=err)
