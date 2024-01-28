#!/usr/bin/env python3
import threading
import requests
from const import MM2_RPC_PORTS, MM2_RPC_HOSTS
from util.files import Files
from util.logger import logger, timed
import util.defaults as default
import util.memcache as memcache
import util.templates as template
import util.transform as transform


class DexAPI:
    def __init__(self, **kwargs):
        try:
            self.kwargs = kwargs
            self.options = []
            default.params(self, self.kwargs, self.options)
            self.netid = "8762"
            self.mm2_host = MM2_RPC_HOSTS[self.netid]
            self.mm2_port = MM2_RPC_PORTS[str(self.netid)]
            self.mm2_rpc = f"{self.mm2_host}:{self.mm2_port}"
            self.files = Files()
        except Exception as e:  # pragma: no cover
            logger.error(f"Failed to init DexAPI: {e}")

    @timed
    def api(self, params: dict) -> dict:
        try:
            r = requests.post(self.mm2_rpc, json=params)
            resp = r.json()
            if "error" not in resp:
                return resp["result"]
            err = {"error": f'{resp["error_type"]} [{resp["error_data"]}]'}
            return err
        except Exception as e:  # pragma: no cover
            logger.warning(params)
            return default.error(e)

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    @timed
    def orderbook_rpc(self, base: str, quote: str, no_cache: bool = False) -> dict:
        try:
            if base != quote:

                if not no_cache:
                    # Get from cache if avialable, otherwise continue
                    cached = memcache.get(f"orderbook_{base}_{quote}")
                    if cached is not None:
                        msg = f"Using cache for {base}_{quote}"
                        return default.result(data=cached, msg=msg, loglevel="loop")

                params = {
                    "mmrpc": "2.0",
                    "method": "orderbook",
                    "params": {"base": base, "rel": quote},
                    "id": 42,
                }
                resp = self.api(params)
                if "error" in resp:
                    msg = f"{resp}: Returning template"
                    resp = template.orderbook(f"{base}_{quote}")
                msg = f"Returning {base}_{quote} orderbook from mm2"
            else:
                msg = f"{base} == {quote}: Returning template"
                resp = template.orderbook(f"{base}_{quote}")

            return default.result(data=resp, msg=msg, loglevel="loop", ignore_until=2)
        except Exception as e:  # pragma: no cover
            data = template.orderbook(pair_str=f"{base}_{quote}")
            msg = f"orderbook rpc failed for {base}_{quote}: {e} {type(e)}. Returning template."
            return default.result(
                data=data, msg=msg, loglevel="warning", ignore_until=0
            )


@timed
def get_orderbook(base, quote):
    try:
        pair = f"{base}_{quote}"
        orderbook_data = DexAPI().orderbook_rpc(base, quote)
        data = transform.label_bids_asks(orderbook_data, pair)
        # Store orderbooks in memory for up to 30 mins
        cache_name = f"orderbook_{pair}"
        memcache.update(cache_name, data, 1800)
        msg = f"{cache_name} added to memcache"
    except Exception as e:  # pragma: no cover
        msg = f"dexapi.get_orderbook {base}/{quote} failed | netid ALL: {e}"
        return default.error(e, msg)
    msg = f"dexapi.get_orderbook {base}/{quote} ok | netid ALL"
    return default.result(data=data, msg=msg)


class OrderbookRpcThread(threading.Thread):
    def __init__(self, base, quote):
        threading.Thread.__init__(self)
        self.base = base
        self.quote = quote

    def run(self):
        get_orderbook(self.base, self.quote)
