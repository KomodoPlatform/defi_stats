#!/usr/bin/env python3
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import List

from db.sqldb import SqlQuery
from db.schema import DefiSwap
from lib.cache import Cache
from models.generic import ErrorMessage, SwapItem
from util.exceptions import UuidNotFoundException
from util.logger import logger

from const import GENERIC_PAIRS_DAYS


router = APIRouter()
cache = Cache()

# These endpoints not yet active. Their intent is to
# expose generic cached data which is reformatted for target specific
# endpoints. For example, the 'markets' and 'gecko' endpoints for ticker
# and pairs are 90% the same, tho might use different keys or value types
# for some items.


@router.get(
    "/get_swaps",
    description=f"Swaps completed within two epoch timestamps.",
    responses={406: {"model": ErrorMessage}},
    response_model=List[DefiSwap],
    status_code=200,
)
def get_swaps(start: int = 0, end: int = 0):
    try:
        query = SqlQuery()
        return query.get_swaps(start=start, end=end)
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)

