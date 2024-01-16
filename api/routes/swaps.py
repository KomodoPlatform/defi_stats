#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
import time
from db.sqldb import SqlQuery
from db.schema import DefiSwap
from models.generic import ErrorMessage
from util.exceptions import UuidNotFoundException, BadPairFormatError
from util.logger import logger

router = APIRouter()



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
            start_time=start_time,
            end_time=end_time,
            coin=coin,
            pair=pair
        )
        if coin is not None:        
            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids['ALL']),
                "swap_uuids": uuids
            }
        elif pair is not None:        
            return {
                "pair": pair,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids['ALL']),
                "swap_uuids": uuids
            }
        return {
                "start_time": start_time,
                "end_time": end_time,
                "swap_count": len(uuids),
                "swap_uuids": uuids
            }
    except BadPairFormatError as e:
        err = {"error": e.name, "message": e.msg}
        return JSONResponse(status_code=e.status_code, content=err)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
