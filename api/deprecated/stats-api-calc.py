#!/usr/bin/env python3
from util.logger import logger
from lib.pair import Pair
from util.enums import NetId


class StatsAPI:
    def __init__(self) -> None:
        pass

    # Data for atomicdex.io website
    def atomicdexio(self, days: int = 1, verbose: bool = True, netid=NetId.ALL) -> dict:
        try:
            DB = self.utils.get_db(self.db_path, self.DB)
            pairs = DB.get_pairs(days)
            logger.muted(
                f"Calculating atomicdexio stats for {len(pairs)} pairs ({days} days)"
            )
            pair_summaries = [
                Pair(pair=i, netid=netid.value).summary(days) for i in pairs
            ]
            current_liquidity = self.utils.get_liquidity(pair_summaries)
            if days == 1:
                data = query.swap_counts()
            else:
                swaps = DB.get_timespan_swaps(days)
                logger.muted(f"{len(swaps)} swaps ({days} days)")
                data = {
                    "days": days,
                    "swaps_count": len(swaps),
                    "swaps_value": self.utils.get_value(pair_summaries),
                    "top_pairs": self.utils.get_top_pairs(pair_summaries),
                }
                data = self.utils.clean_decimal_dict(data)
            data.update({"current_liquidity": round(float(current_liquidity), 8)})
            return data
        except Exception as e:
            logger.error(f"{type(e)} Error in [Cache.calc.atomicdexio]: {e}")
            return None
