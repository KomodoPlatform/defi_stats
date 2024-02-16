#!/usr/bin/env python3
from decimal import Decimal
from typing import Dict
import requests
import threading
from const import MM2_RPC_PORTS, MM2_RPC_HOSTS, API_ROOT_PATH
from util.cron import cron
from util.files import Files
from util.logger import logger, timed
from util.transform import sortdata, clean, invert, derive, template, convert
import lib.prices as prices
import util.defaults as default
import util.memcache as memcache
import util.validate as validate


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
            return default.result(msg=e, loglevel="warning")

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    @timed
    def orderbook_rpc(self, base: str, quote: str) -> dict:
        """Either returns template, or actual result"""
        try:
            params = {
                "mmrpc": "2.0",
                "method": "orderbook",
                "params": {"base": base, "rel": quote},
                "id": 42,
            }
            resp = self.api(params)
            if "error" in resp:
                raise ValueError
            msg = f"Returning {base}_{quote} orderbook from mm2"
            return default.result(data=resp, msg=msg, loglevel="loop", ignore_until=3)
        except Exception as e:  # pragma: no cover
            data = template.orderbook_rpc_resp(base=base, quote=quote)
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
        except Exception as e:  # pragma: no cover
            logger.warning(e)

    @timed
    def run(self):
        try:
            data = DexAPI().orderbook_rpc(self.base, self.quote)
            data = orderbook_extras(
                pair_str=self.pair_str, data=data, gecko_source=self.gecko_source
            )
            # update the variant cache. Double expiry vs combined to
            # make sure variants are never empty when combined asks.
            data = clean.decimal_dicts(data)
            memcache.update(self.variant_cache_name, data, 900)
        except Exception as e:  # pragma: no cover
            logger.warning(e)
        return default.result(
            data=data,
            ignore_until=3,
            msg=f"Threaded orderbook for {self.pair_str} complete",
            loglevel="loop",
        )


@timed
def get_orderbook(
    base: str,
    quote: str,
    coins_config: Dict,
    gecko_source: Dict,
    variant_cache_name: str,
    depth: int = 100,
    refresh=False,
):
    try:
        """
        If `refresh` is true request is threaded and added to cache.
        If `refresh` is false, resp from cache or standard request.
        """
        ignore_until = 2
        pair_str = f"{base}_{quote}"
        if not validate.orderbook_request(
            base=base, quote=quote, coins_config=coins_config
        ):
            raise ValueError
        if memcache.get("testing") is not None:
            return get_orderbook_fixture(pair_str, gecko_source=gecko_source)
        if refresh:
            t = OrderbookRpcThread(
                base,
                quote,
                variant_cache_name=variant_cache_name,
                depth=depth,
                gecko_source=gecko_source,
            )
            t.start()
            return None

        # Use variant cache if available
        cached = memcache.get(variant_cache_name)
        if cached is not None:
            data = cached
            msg = f"Returning orderbook for {pair_str} from cache"
        else:
            data = DexAPI().orderbook_rpc(base, quote)
            data = orderbook_extras(
                pair_str=pair_str, data=data, gecko_source=gecko_source
            )
            data = clean.decimal_dicts(data)
            memcache.update(variant_cache_name, data, 900)
            msg = f"Updated orderbook cache for {pair_str}"
        ignore_until = 3
    except Exception as e:  # pragma: no cover
        data = template.orderbook_extended(pair_str=pair_str)
        data = orderbook_extras(pair_str=pair_str, data=data, gecko_source=gecko_source)
        ignore_until = 0
        msg = f"dex_api.get_orderbook {pair_str} failed: {e}! Returning template"
    return default.result(
        data=data,
        ignore_until=ignore_until,
        msg=msg,
        loglevel="loop",
    )


@timed
def get_orderbook_fixture(pair_str, gecko_source):
    files = Files()
    path = f"{API_ROOT_PATH}/tests/fixtures/orderbook"
    fn = f"{pair_str}.json"
    fixture = files.load_jsonfile(f"{path}/{fn}")
    if fixture is None:
        fn = f"{invert.pair(pair_str)}.json"
        fixture = files.load_jsonfile(f"{path}/{fn}")
    if fixture is not None:
        data = fixture
    else:
        logger.warning(f"fixture for {pair_str} does not exist!")
        base, quote = derive.base_quote(pair_str=pair_str)
        data = template.orderbook_rpc_resp(base=base, quote=quote)
    is_reversed = pair_str != sortdata.pair_by_market_cap(pair_str)
    if is_reversed:
        data = invert.orderbook_fixture(data)
    data = orderbook_extras(pair_str, data, gecko_source)
    if len(data["bids"]) > 0 or len(data["asks"]) > 0:
        data = clean.decimal_dicts(data)
    return default.result(
        data=data,
        ignore_until=2,
        msg=f"Got fixture for {pair_str}",
        loglevel="loop",
    )


@timed
def orderbook_extras(pair_str, data, gecko_source):
    try:
        data["pair"] = pair_str
        base, quote = derive.base_quote(pair_str=pair_str)
        data["base"] = base
        data["quote"] = quote
        data["timestamp"] = int(cron.now_utc())
        data = convert.label_bids_asks(data)
        # Exctract volume values
        for i in data:
            if isinstance(data[i], dict):
                if "decimal" in data[i]:
                    data[i] = data[i]["decimal"]
        data = get_liquidity(data, gecko_source)
        price_data = prices.pair_price_24hr_cache(pair_str=pair_str)
        data.update(
            {
                "trades_24hr": price_data["swaps"],
                "oldest_price_24hr": price_data["oldest_price"],
                "oldest_price_time": price_data["oldest_price_time"],
                "newest_price_24hr": price_data["newest_price"],
                "newest_price_time": price_data["newest_price_time"],
                "highest_price_24hr": price_data["highest_price_24hr"],
                "lowest_price_24hr": price_data["lowest_price_24hr"],
                "price_change_pct_24hr": price_data["price_change_pct_24hr"],
                "price_change_24hr": price_data["price_change_24hr"],
                "base_price_usd": price_data["base_price_usd"],
                "quote_price_usd": price_data["quote_price_usd"],
                "trade_volume_usd": price_data["trade_volume_usd"],
                "highest_bid": derive.highest_bid(data),
                "lowest_ask": derive.lowest_ask(data),
            }
        )
    except Exception as e:  # pragma: no cover
        loglevel = "warning"
        ignore_until = 0
        msg = f"Orderbook.extras failed for {pair_str}: {e}"
    ignore_until = 5
    loglevel = "dexrpc"
    msg = f"Got Orderbook.extras for {pair_str}"
    return default.result(
        data=data, msg=msg, loglevel=loglevel, ignore_until=ignore_until
    )


@timed
def get_liquidity(orderbook, gecko_source):
    """Liquidity for pair from current orderbook & usd price."""
    try:
        # Prices and volumes
        orderbook.update(
            {
                "base_price_usd": derive.gecko_price(
                    orderbook["base"], gecko_source=gecko_source
                ),
                "quote_price_usd": derive.gecko_price(
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
        # Get combined liquidity for the pair
        orderbook.update(
            {
                "liquidity_usd": orderbook["base_liquidity_usd"]
                + orderbook["quote_liquidity_usd"],
            }
        )
        msg = f"Got Liquidity for {orderbook['pair']}"
        return default.result(data=orderbook, msg=msg, loglevel="calc", ignore_until=3)
    except Exception as e:  # pragma: no cover
        msg = f"Returning liquidity template for {orderbook['pair']} ({e})"
        return default.result(
            data=orderbook, msg=msg, loglevel="warning", ignore_until=0
        )
