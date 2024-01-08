#!/usr/bin/env python3
from util.logger import logger


def get_top_pairs(self, pairs_data: list):
    try:
        pairs_data.sort(key=lambda x: x["pair_trade_value_usd"], reverse=True)
        value_data = pairs_data[:5]
        top_pairs_by_value = {}
        [
            top_pairs_by_value.update({i["trading_pair"]: i["pair_trade_value_usd"]})
            for i in value_data
        ]
        pairs_data.sort(key=lambda x: x["pair_liquidity_usd"], reverse=True)
        liquidity_data = pairs_data[:5]
        top_pairs_by_liquidity = {}
        [
            top_pairs_by_liquidity.update({i["trading_pair"]: i["pair_liquidity_usd"]})
            for i in liquidity_data
        ]
        pairs_data.sort(key=lambda x: x["pair_swaps_count"], reverse=True)
        swaps_data = pairs_data[:5]
        top_pairs_by_swaps = {}
        [
            top_pairs_by_swaps.update({i["trading_pair"]: i["pair_swaps_count"]})
            for i in swaps_data
        ]

        return {
            "by_value_traded_usd": self.clean_decimal_dict(top_pairs_by_value),
            "by_current_liquidity_usd": self.clean_decimal_dict(top_pairs_by_liquidity),
            "by_swaps_count": self.clean_decimal_dict(top_pairs_by_swaps),
        }
    except Exception as e:
        logger.error(f"{type(e)} Error in [get_top_pairs]: {e}")
        return {"by_volume": [], "by_liquidity": [], "by_swaps": []}
