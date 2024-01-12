from sqlalchemy import Table, Column, Integer, String, MetaData, DECIMAL

meta = MetaData()

stats_swaps = Table(
    "stats_swaps",
    meta,
    Column("id", Integer, primary_key=True),
    Column("maker_coin", String(20), nullable=False),
    Column("taker_coin", String(20), nullable=False),
    Column("uuid", String(36), nullable=False, unique=True),
    Column("started_at", Integer, nullable=False),
    Column("finished_at", Integer, nullable=False),
    Column("maker_amount", DECIMAL, nullable=False),
    Column("taker_amount", DECIMAL, nullable=False),
    Column("is_success", Integer, nullable=False),
    Column("maker_coin_ticker", String(10), nullable=False),
    Column("maker_coin_platform", String(10), nullable=False),
    Column("taker_coin_ticker", String(10), nullable=False),
    Column("taker_coin_platform", String(10), nullable=False),
    Column("maker_coin_usd_price", DECIMAL, nullable=False),
    Column("taker_coin_usd_price", DECIMAL, nullable=False),
    Column("maker_pubkey", String(72), nullable=False),
    Column("taker_pubkey", String(72), nullable=False),
)
