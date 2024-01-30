#!/usr/bin/env python3
from decimal import Decimal
import requests
import threading
from typing import Dict

from const import MM2_RPC_PORTS, MM2_RPC_HOSTS, API_ROOT_PATH
from lib.coins import get_gecko_price
import util.defaults as default
import util.memcache as memcache
import util.templates as template
import util.transform as transform
from util.files import Files
from util.logger import logger, timed
from util.transform import sortdata, clean


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
    def orderbook_rpc(self, base: str, quote: str) -> dict:
        """Either returns template, or actual result"""
        try:
            if base != quote:
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


class OrderbookRpcThread(threading.Thread):
    def __init__(self, base, quote, variant_cache_name, depth, gecko_source):
        threading.Thread.__init__(self)
        try:
            self.base = base
            self.quote = quote
            self.gecko_source = gecko_source
            self.pair_str = f"{self.base}_{self.quote}"
            self.variant_cache_name = variant_cache_name
            self.depth = depth
        except Exception as e:
            logger.warning(e)

    def run(self):
        try:
            data = DexAPI().orderbook_rpc(self.base, self.quote)
            # logger.calc(data)
            data["pair"] = self.pair_str
            data = transform.label_bids_asks(data, self.pair_str)
            data = get_liquidity(data, self.gecko_source)
            # logger.query(data)
            data["variants"] = []
            
            # update the variant cache. Double expiry vs combined to
            # make sure variants are never empty when comnbined asks.
            if len(data["bids"]) > 0 or len(data["asks"]) > 0:
                data = clean.decimal_dict(data)
                memcache.update(self.variant_cache_name, data, 240)
        except Exception as e:
            logger.warning(e)


@timed
def get_orderbook(
    base: str,
    quote: str,
    coins_config: Dict,
    gecko_source: Dict,
    variant_cache_name: str,
    depth: int = 100,
):
    try:
        pair_str = f"{base}_{quote}"
        if base not in coins_config or quote not in coins_config:
            msg = f"dex_api.get_orderbook {base} not in coins_config!"
        if quote not in coins_config:
            msg = f"dex_api.get_orderbook {quote} not in coins_config!"
        elif coins_config[base]["wallet_only"]:
            msg = f"dex_api.get_orderbook {base} is wallet only!"
        elif coins_config[quote]["wallet_only"]:
            msg = f"dex_api.get_orderbook {quote} is wallet only!"
        elif memcache.get("testing") is not None:
            get_orderbook_fixture(pair_str)
        else:
            # Use variant cache if available
            cached = memcache.get(variant_cache_name)
            if cached is not None and (
                len(cached["asks"]) > 0 or len(cached["bids"]) > 0
            ):
                return default.result(
                    data=cached, loglevel="loop ", ignore_until=2, msg=msg
                )
            t = OrderbookRpcThread(
                base,
                quote,
                variant_cache_name=variant_cache_name,
                depth=depth,
                gecko_source=gecko_source,
            )
            t.start()
        msg = f"Returning orderbook template for {pair_str} while cache reloads"
    except Exception as e:  # pragma: no cover
        msg = f"dex_api.get_orderbook {pair_str} failed: {e}! Returning template"
    return default.result(
        data=template.orderbook(pair_str=pair_str),
        ignore_until=2,
        msg=msg,
        loglevel="loop",
    )


def get_orderbook_fixture(pair_str):
    files = Files()
    path = f"{API_ROOT_PATH}/tests/fixtures/orderbook"
    fn = f"{pair_str}.json"
    fixture = files.load_jsonfile(f"{path}/{fn}")
    if fixture is None:
        fn = f"{transform.invert_pair(pair_str)}.json"
        fixture = files.load_jsonfile(f"{path}/{fn}")
    if fixture is not None:
        data = fixture
    is_reversed = pair_str != sortdata.order_pair_by_market_cap(pair_str)
    data["pair"] = pair_str
    if is_reversed:
        data = transform.invert_orderbook(data)
    return transform.label_bids_asks(data, pair_str)


@timed
def get_liquidity(orderbook, gecko_source):
    """Liquidity for pair from current orderbook & usd price."""
    try:
        # logger.loop(orderbook)
        # Prices and volumes
        orderbook.update(
            {
                "base_price_usd": get_gecko_price(
                    orderbook["base"], gecko_source=gecko_source
                ),
                "quote_price_usd": get_gecko_price(
                    orderbook["quote"], gecko_source=gecko_source
                ),
                "total_asks_base_vol": sum(
                    [Decimal(i["volume"]) for i in orderbook["asks"]]
                ),
                "total_bids_base_vol": sum(
                    [Decimal(i["volume"]) for i in orderbook["bids"]]
                ),
                "total_asks_quote_vol": sum(
                    [Decimal(i["quote_volume"]) for i in orderbook["asks"]]
                ),
                "total_bids_quote_vol": sum(
                    [Decimal(i["quote_volume"]) for i in orderbook["bids"]]
                ),
            }
        )
        # logger.calc(orderbook)
        # TODO: Some duplication here, could be reduced.
        orderbook.update(
            {
                "base_liquidity_coins": orderbook["total_asks_base_vol"],
                "base_liquidity_usd": orderbook["total_asks_base_vol"]
                * orderbook["base_price_usd"],
                "quote_liquidity_coins": orderbook["total_bids_quote_vol"],
                "quote_liquidity_usd": orderbook["total_bids_quote_vol"]
                * orderbook["quote_price_usd"],
                "total_asks_base_usd": orderbook["total_asks_base_vol"]
                * orderbook["base_price_usd"],
                "total_bids_quote_usd": orderbook["total_bids_quote_vol"]
                * orderbook["quote_price_usd"],
            }
        )
        # logger.query(orderbook)
        # Get combined liquidity for the pair
        orderbook.update(
            {
                "liquidity_in_usd": orderbook["base_liquidity_usd"]
                + orderbook["quote_liquidity_usd"],
            }
        )
        # logger.loop(orderbook)
        msg = f"Got Liquidity for {orderbook['pair']}"
        return default.result(data=orderbook, msg=msg, loglevel="calc", ignore_until=2)
    except Exception as e:  # pragma: no cover
        msg = f"Returning liquidity template for {orderbook['pair']} ({e})"
        return default.result(
            data=orderbook, msg=msg, loglevel="warning", ignore_until=0
        )
