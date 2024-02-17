#!/usr/bin/env python3
from typing import Dict
from util.logger import timed
from util.transform import deplatform, invert, template
import util.defaults as default
import util.memcache as memcache


@timed
def pair_last_trade_cache(pair_str: str, last_data: Dict):  # pragma: no cover
    try:
        depair = deplatform.pair(pair_str)
        data = template.first_last_traded()
        if last_data is not None:
            if depair in last_data:
                if pair_str in last_data[depair]:
                    v = last_data[depair][pair_str]
                    data.update(v)
            elif invert.pair(depair) in last_data:
                if invert.pair(pair_str) in last_data[invert.pair(depair)]:
                    v = last_data[invert.pair(depair)][invert.pair(pair_str)]
                    data.update(v)
        return default.result(
            data=data,
            msg=f"{pair_str} complete",
            loglevel="cached",
            ignore_until=3,
        )
    except Exception as e:
        msg = f"{pair_str} failed: {e}!"
        try:
            data = template.first_last_traded()
            msg += " Returning template!"
        except Exception as e:
            data = {"error": f"{msg}: {e}"}
        return default.result(data=data, msg=msg, loglevel="warning", ignore_until=0)
