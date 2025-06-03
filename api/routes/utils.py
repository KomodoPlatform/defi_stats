#!/usr/bin/env python3
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from models.generic import ErrorMessage
from util.logger import logger
from util.validate import Blacklist
from util.client_ip import get_client_ip, get_all_ip_headers, get_client_location

router = APIRouter()


@router.get(
    "/bouncer",
    description="Checks client IP address against IP2Location LITE database to restrict blacklisted regions.",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def bouncer(request: Request):
    """
    Check if client IP is restricted. 
    If ip_address is provided, check that IP instead of client IP.
    """
    try:
        # Get client IP if not provided
        client_ip = get_client_ip(request)
        location = get_client_location(client_ip)
        blacklist = Blacklist()
        logger.info(f"Client IP: {client_ip}, Location: {location}")
        if blacklist.is_restricted(client_ip):
            logger.warning(f"Client IP: {client_ip} from {location} is restricted")
            return JSONResponse(status_code=403, content={"error": "Verboten"})
        return JSONResponse(status_code=200, content={"message": "OK"})
    except Exception as e:  # pragma: no cover
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


