class MarketsDesc:
    def __init__(self) -> None:
        pass

    @property
    def adexio(self):
        return "Returns atomic swap counts over a variety of periods"

    @property
    def liquidity(self):
        desc = "Global liquidity on the orderbook for all pairs traded in the last 30 days."
        desc += (
            " There may be additional liquidity from pairs traded prior to this time."
        )
        return desc

    @property
    def orderbook(self):
        desc = "Current orderbook for a given pair, or all variants of a pair (including tokens)."
        desc += " Use `KMD-BEP20_LTC` format."
        return desc


markets_desc = MarketsDesc()
