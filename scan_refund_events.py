#!/usr/bin/env python3
"""Scan JSON trade logs for refund-related events."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


REFUND_EVENTS = {
    "MakerPaymentRefunded",
    "MakerPaymentWaitRefundStarted",
    "MakerPaymentRefundFailed",
    "TakerPaymentWaitRefundStarted",
    "TakerPaymentRefunded",
    "TakerPaymentRefundFailed",
}


def iter_json_files(root: Path) -> Iterable[Path]:
    """Yield all JSON files under root."""
    for path in root.rglob("*.json"):
        if path.is_file():
            yield path


def load_json(path: Path) -> dict | None:
    """Load JSON file, returning None on failure."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:  # noqa: BLE001 - want to report all failures
        print(f"Failed to load {path}: {exc}", file=sys.stderr)
        return None


def ticker_matches(payload: dict, ticker: str | None) -> bool:
    """Return True when ticker filter is disabled or matches maker/taker coins."""
    if ticker is None:
        return True

    ticker_upper = ticker.upper()
    maker_coin = str(payload.get("maker_coin", "")).upper()
    taker_coin = str(payload.get("taker_coin", "")).upper()
    return ticker_upper in (maker_coin, taker_coin)


def normalize_timestamp(value) -> int | None:
    """Return timestamp in milliseconds when possible."""
    if isinstance(value, (int, float)):
        # Heuristic: values above this threshold are assumed to already be ms.
        if value >= 1_000_000_000_000:
            return int(value)
        return int(value * 1000)
    return None


def find_refund_events(payload: dict, cutoff_ms: int | None) -> list[dict]:
    """Extract refund events from payload."""
    events = payload.get("events", [])
    matching: list[dict] = []
    for entry in events:
        event = entry.get("event") or {}
        event_type = event.get("type")
        if event_type in REFUND_EVENTS:
            timestamp_raw = entry.get("timestamp")
            timestamp_ms = normalize_timestamp(timestamp_raw)
            if cutoff_ms is not None and (
                timestamp_ms is None or timestamp_ms < cutoff_ms
            ):
                continue
            matching.append(
                {
                    "timestamp": timestamp_raw,
                    "type": event_type,
                    "data": event.get("data"),
                }
            )
    return matching


def scan(root: Path, ticker: str | None, max_age_days: int | None) -> int:
    """Scan for refund events; return count of files with matches."""
    cutoff_ms = None
    if max_age_days is not None:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
        cutoff_ms = int(cutoff.timestamp() * 1000)

    matched_files = 0
    for json_path in sorted(iter_json_files(root)):
        payload = load_json(json_path)
        if not payload or not ticker_matches(payload, ticker):
            continue

        matches = find_refund_events(payload, cutoff_ms)
        if not matches:
            continue

        matched_files += 1
        maker_coin = payload.get("maker_coin", "UNKNOWN")
        taker_coin = payload.get("taker_coin", "UNKNOWN")
        print(f"{json_path} [{maker_coin}/{taker_coin}]:")
        for match in matches:
            timestamp = match.get("timestamp")
            event_type = match.get("type")
            print(f"  - {timestamp}: {event_type}")
        print()

    if matched_files == 0:
        print("No matching events found.")

    return matched_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan JSON logs for refund-related events."
    )
    parser.add_argument(
        "root",
        type=Path,
        help="Directory to scan recursively for .json files.",
    )
    parser.add_argument(
        "--ticker",
        help="Filter results by maker or taker coin ticker.",
    )
    parser.add_argument(
        "--days",
        type=int,
        dest="max_age_days",
        help="Only consider events that occurred within the last N days.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Directory not found: {root}", file=sys.stderr)
        raise SystemExit(1)

    scan(root, args.ticker, args.max_age_days)


if __name__ == "__main__":
    main()

