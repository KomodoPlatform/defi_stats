#!/usr/bin/env python3
import requests
from util.files import Files
from const import API_ROOT_PATH, MM2_RPC_PORTS, MM2_RPC_HOSTS, IS_TESTING
from util.logger import logger, timed
from util.defaults import set_params, default_error
import util.templates as template


class DexAPI:
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["testing", "netid"]
            set_params(self, self.kwargs, self.options)
            if IS_TESTING:
                self.testing = True
            if self.netid == "ALL":  # pragma: no cover
                self.netid = "8762"
            if self.testing:
                self.mm2_host = "http://127.0.0.1"
            else:
                self.mm2_host = MM2_RPC_HOSTS[self.netid]
            self.mm2_port = MM2_RPC_PORTS[self.netid]
            self.mm2_rpc = f"{self.mm2_host}:{self.mm2_port}"
            self.files = Files(testing=self.testing)
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init DexAPI: {e}")

    @timed
    def api(self, params: dict) -> dict:
        try:
            # logger.calc(params)
            r = requests.post(self.mm2_rpc, json=params)
            resp = r.json()
            if "error" not in resp:
                return resp["result"]
            err = {"error": f'{resp["error_type"]} [{resp["error_data"]}]'}
            return err
        except Exception as e:  # pragma: no cover
            logger.warning(params)
            return default_error(e)

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    @timed
    def orderbook_rpc(self, base: str, quote: str) -> dict:
        try:
            if self.testing:
                orderbook = (
                    f"{API_ROOT_PATH}/tests/fixtures/orderbook/{base}_{quote}.json"
                )
                return self.files.load_jsonfile(orderbook)

            params = {
                "mmrpc": "2.0",
                "method": "orderbook",
                "params": {"base": base, "rel": quote},
                "id": 42,
            }
            resp = self.api(params)
            if "error" in resp:
                logger.muted(f"{resp['error']}: Returning template")
                return template.orderbook(f"{base}_{quote}")
            return resp

        except Exception as e:  # pragma: no cover
            return default_error(e)
