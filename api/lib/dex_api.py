#!/usr/bin/env python3
import requests
from util.files import Files
from const import API_ROOT_PATH, MM2_RPC_PORTS, MM2_RPC_HOSTS
from util.logger import logger, timed
from util.defaults import set_params, default_error, default_result
import util.templates as template


class DexAPI:
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = ["netid"]
            set_params(self, self.kwargs, self.options)

            if self.netid == "ALL":  # pragma: no cover
                self.netid = "8762"
            self.mm2_host = MM2_RPC_HOSTS[self.netid]
            self.mm2_port = MM2_RPC_PORTS[self.netid]
            self.mm2_rpc = f"{self.mm2_host}:{self.mm2_port}"
            self.files = Files()
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


@timed
def get_orderbook(base, quote):
    try:
        pair = f"{base}_{quote}"
        data = template.orderbook(pair)
        dexapi = DexAPI(netid="ALL")
        x = dexapi.orderbook_rpc(base, quote)
        for i in ["asks", "bids"]:
            items = [
                {
                    "price": j["price"]["decimal"],
                    "volume": j["base_max_volume"]["decimal"],
                }
                for j in x[i]
            ]
            x[i] = items
            data[i] += x[i]
    except Exception as e:  # pragma: no cover
        msg = f"orderbook.get_and_parse {base}/{quote} failed | netid ALL: {e}"
        return default_error(e, msg)
    msg = f"orderbook.get_and_parse {base}/{quote} ok | netid ALL"
    return default_result(data=data, msg=msg)
