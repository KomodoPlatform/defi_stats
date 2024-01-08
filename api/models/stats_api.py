from pydantic import BaseModel
from typing import Dict


# stats-api/atomicdexio
class StatsApiAtomicdexIo(BaseModel):
    swaps_24hr: int = 777
    swaps_30d: int = 777777
    swaps_all_time: int = 777777777777
    current_liquidity: float = 7777777.777


# stats-api/atomicdex_fortnight
class TopPairs(BaseModel):
    by_value_traded_usd: dict = Dict[str, float]
    by_current_liquidity_usd: dict = Dict[str, float]
    by_swaps_count: dict = Dict[str, int]


class StatsApiAtomicdexFortnight(BaseModel):
    days: int = 777
    swaps_count: int = 777777
    swaps_value: int = 777777777777
    current_liquidity: float = 7777777.777
    top_pairs: dict = TopPairs
