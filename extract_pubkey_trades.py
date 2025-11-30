#!/usr/bin/env python3
"""
CLI to extract trades for a given pubkey, with optional time and coin/pair filters.

Examples:
  - All-time trades for pubkey (JSON to stdout):
      ./extract_pubkey_trades.py --pubkey <PUBKEY>

  - Between timestamps, filtered by coin ticker (CSV to file):
      ./extract_pubkey_trades.py --pubkey <PUBKEY> --start 1704067200 --end 1706745600 \
        --coin KMD --out csv --output exports/kmd_pubkey_trades.csv

  - Filtered by pair (any variant or std):
      ./extract_pubkey_trades.py --pubkey <PUBKEY> --pair KMD_LTC --out json \
        --output exports/kmd_ltc_pubkey_trades.json
"""

import os
import sys
import json
import csv
import argparse
from datetime import datetime, timezone

# Add the API root to the path to import existing modules
API_ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api")
sys.path.append(API_ROOT_PATH)

def _load_env():
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(API_ROOT_PATH, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
    except Exception:
        pass

_load_env()
os.environ.setdefault("IS_TESTING", "False")

try:
    from sqlmodel import Session, select, or_
    from db.sqldb import SqlDB
    from db.schema import DefiSwap
except Exception as e:
    print(f"Error importing API modules: {e}")
    print("Run this from the repo root and ensure dependencies are installed.")
    sys.exit(1)


def _build_query(session: Session, pubkey: str, start_ts: int, end_ts: int,
                 coin: str | None, pair: str | None):
    q = select(DefiSwap)

    # Time range (finished_at)
    q = q.where(DefiSwap.finished_at > start_ts, DefiSwap.finished_at < end_ts)

    # Pubkey on either side
    q = q.where(or_(DefiSwap.maker_pubkey == pubkey, DefiSwap.taker_pubkey == pubkey))

    # Optional coin filter: match by variant or ticker on either side
    if coin:
        coin_in = coin.strip()
        coin_ticker = coin_in.upper()
        q = q.where(
            or_(
                DefiSwap.maker_coin == coin_in,
                DefiSwap.taker_coin == coin_in,
                DefiSwap.maker_coin_ticker == coin_ticker,
                DefiSwap.taker_coin_ticker == coin_ticker,
            )
        )

    # Optional pair filter: accept std (BASE_QUOTE) or variant (BASE-PROTO_QUOTE-PROTO)
    if pair:
        pair_in = pair.strip().upper()
        q = q.where(
            or_(
                DefiSwap.pair == pair_in,
                DefiSwap.pair_reverse == pair_in,
                DefiSwap.pair_std == pair_in,
                DefiSwap.pair_std_reverse == pair_in,
            )
        )

    # Order newest first
    q = q.order_by(DefiSwap.finished_at.desc())
    return q


def _rows_to_dicts(rows):
    data = []
    for r in rows:
        # SQLModel instances; convert to dict and drop SA internals
        try:
            d = dict(r)
        except Exception:
            d = getattr(r, "__dict__", {}).copy()
        if not isinstance(d, dict):
            d = {}
        # Remove SQLAlchemy internals
        for k in list(d.keys()):
            if k.startswith("_sa_"):
                d.pop(k, None)
        data.append(d)
    return data


def export_json(rows: list[dict], output_path: str | None):
    # Add human-readable timestamps
    enriched = []
    for row in rows:
        item = dict(row)
        for tfield in ["started_at", "finished_at"]:
            ts = item.get(tfield)
            if isinstance(ts, int) and ts > 0:
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                item[f"{tfield}_readable"] = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        enriched.append(item)

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, indent=2, default=str)
        print(f"Wrote {len(rows)} trades to {output_path}")
    else:
        print(json.dumps(enriched, indent=2, default=str))


def export_csv(rows: list[dict], output_path: str | None):
    fieldnames = [
        "uuid", "pair", "pair_std", "started_at", "finished_at", "duration",
        "maker_coin", "maker_coin_ticker", "maker_amount", "maker_coin_usd_price",
        "taker_coin", "taker_coin_ticker", "taker_amount", "taker_coin_usd_price",
        "price", "reverse_price", "is_success", "maker_gui", "taker_gui",
        "maker_version", "taker_version", "maker_pubkey", "taker_pubkey",
    ]

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        out = open(output_path, "w", newline="", encoding="utf-8")
        close_after = True
    else:
        out = sys.stdout
        close_after = False

    try:
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        if output_path:
            print(f"Wrote {len(rows)} trades to {output_path}")
    finally:
        if close_after:
            out.close()


def main():
    parser = argparse.ArgumentParser(description="Extract trades for a pubkey")
    parser.add_argument("--pubkey", required=True, help="Maker or taker pubkey")
    parser.add_argument("--start", type=int, default=0, help="Start UNIX timestamp (inclusive)")
    parser.add_argument("--end", type=int, default=0, help="End UNIX timestamp (exclusive)")
    parser.add_argument("--coin", type=str, default=None, help="Filter by coin ticker or variant")
    parser.add_argument("--pair", type=str, default=None, help="Filter by pair (std or variant)")
    parser.add_argument("--out", choices=["json", "csv"], default="json", help="Output format")
    parser.add_argument("--output", type=str, default=None, help="Output path (default: stdout for JSON)")

    args = parser.parse_args()

    # Default time range: all-time if none provided
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    start_ts = args.start if args.start and args.start > 0 else 1
    end_ts = args.end if args.end and args.end > 0 else now_ts

    # Connect to Postgres (default) via existing engine
    db = SqlDB(db_type="pgsql")

    with Session(db.engine) as session:
        q = _build_query(
            session=session,
            pubkey=args.pubkey,
            start_ts=start_ts,
            end_ts=end_ts,
            coin=args.coin,
            pair=args.pair,
        )
        rows = session.exec(q).all()

    data = _rows_to_dicts(rows)

    if args.out == "json":
        export_json(data, args.output)
    else:
        export_csv(data, args.output or os.path.join("exports", "pubkey_trades.csv"))


if __name__ == "__main__":
    main()



