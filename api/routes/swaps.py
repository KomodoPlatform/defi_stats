#!/usr/bin/env python3
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from const import MM2_DB_PATH_SEED
from db.schema import Mm2StatsNodes
from models.generic import ErrorMessage, MonthlyStatsResponse, MonthlyStatsItem, MonthlyPairStats, MonthlyPubkeyStats, MonthlyGuiStats
from util.cron import cron
from util.exceptions import UuidNotFoundException, BadPairFormatError
from util.logger import logger
import db.sqldb as db
from collections import Counter, defaultdict
from decimal import Decimal
from datetime import datetime

router = APIRouter()


@router.get(
    "/swap/{uuid}",
    description="Get swap info from a uuid, e.g. `82df2fc6-df0f-439a-a4d3-efb42a3c1db8`",
    responses={406: {"model": ErrorMessage}},
    response_model=db.DefiSwap,
    status_code=200,
)
def get_swap(uuid: str):
    try:
        query = db.SqlQuery()
        resp = query.get_swap(uuid=uuid)
        if "error" in resp:
            raise UuidNotFoundException(resp["error"])
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swap_uuids",
    description="Get swap uuids for a pair (e.g. `KMD_LTC`).",
    responses={406: {"model": ErrorMessage}},
    status_code=200,
)
def swap_uuids(
    start_time: int = 0,
    end_time: int = 0,
    coin: str | None = None,
    pair: str | None = None,
):
    try:
        if start_time == 0:
            start_time = int(cron.now_utc()) - 86400
        if end_time == 0:
            end_time = int(cron.now_utc())
        query = db.SqlQuery()
        uuids = query.swap_uuids(
            start_time=start_time, end_time=end_time, coin=coin, pair=pair
        )
        if coin is not None:
            return {
                "coin": coin,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        elif pair is not None:
            return {
                "pair": pair,
                "start_time": start_time,
                "end_time": end_time,
                "variants": list(uuids.keys()),
                "swap_count": len(uuids["ALL"]),
                "swap_uuids": uuids,
            }
        return {
            "start_time": start_time,
            "end_time": end_time,
            "swap_count": len(uuids),
            "swap_uuids": uuids,
        }
    except BadPairFormatError as e:
        err = {"error": e.name, "message": e.msg}
        return JSONResponse(status_code=e.status_code, content=err)
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)

@router.get(
    "/monthly_stats/{year}",
    description="Monthly swap stats for a given year, with optional filtering by pubkey or gui.",
    response_model=MonthlyStatsResponse,
    status_code=200,
)
def monthly_stats(
    year: int,
    pubkey: str | None = None,
    gui: str | None = None,
):
    """
    Returns monthly stats for the given year, optionally filtered by pubkey or gui.
    Optimized to use SQL aggregation for performance.
    """
    try:
        return JSONResponse(status_code=402, content=err)
        query = db.SqlQuery()
        from sqlalchemy import text
        from datetime import datetime
        start_dt = datetime(year, 1, 1)
        end_dt = datetime(year + 1, 1, 1)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        # Filters for pubkey/gui
        pubkey_filter = "" if not pubkey else f"AND (maker_pubkey = :pubkey OR taker_pubkey = :pubkey)"
        gui_filter = "" if not gui else f"AND (maker_gui = :gui OR taker_gui = :gui)"
        params = {"start_ts": start_ts, "end_ts": end_ts}
        if pubkey:
            params["pubkey"] = pubkey
        if gui:
            params["gui"] = gui
        # 1. Total swaps per month
        sql_swaps = text(f'''
            SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, COUNT(*) AS swap_count
            FROM defi_swaps
            WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter}
            GROUP BY month
        ''')
        # 2. Top pairs per month (by count and volume)
        sql_pairs = text(f'''
            SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, pair_std, COUNT(*) AS swap_count, SUM(maker_amount + taker_amount) AS volume
            FROM defi_swaps
            WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter}
            GROUP BY month, pair_std
        ''')
        # 3. Unique pubkeys per month
        sql_pubkeys = text(f'''
            SELECT month, COUNT(DISTINCT pubkey) AS unique_pubkeys FROM (
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, maker_pubkey AS pubkey
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND maker_pubkey IS NOT NULL AND maker_pubkey != 'unknown'
                UNION ALL
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, taker_pubkey AS pubkey
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND taker_pubkey IS NOT NULL AND taker_pubkey != 'unknown'
            ) t
            GROUP BY month
        ''')
        # 4. Top pubkeys per month (by count and volume)
        sql_top_pubkeys = text(f'''
            SELECT month, pubkey, COUNT(*) AS swap_count, SUM(maker_amount + taker_amount) AS volume FROM (
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, maker_pubkey AS pubkey, maker_amount, taker_amount
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND maker_pubkey IS NOT NULL AND maker_pubkey != 'unknown'
                UNION ALL
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, taker_pubkey AS pubkey, maker_amount, taker_amount
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND taker_pubkey IS NOT NULL AND taker_pubkey != 'unknown'
            ) t
            GROUP BY month, pubkey
        ''')
        # 5. GUI stats per month (swap count and unique pubkeys)
        sql_gui_stats = text(f'''
            SELECT month, gui, COUNT(*) AS swap_count, COUNT(DISTINCT pubkey) AS pubkey_count FROM (
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, maker_gui AS gui, maker_pubkey AS pubkey
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND maker_gui IS NOT NULL AND maker_gui != 'unknown'
                UNION ALL
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, taker_gui AS gui, taker_pubkey AS pubkey
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND taker_gui IS NOT NULL AND taker_gui != 'unknown'
            ) t
            GROUP BY month, gui
        ''')
        # 6. Pubkey GUI counts (how many GUIs each pubkey used per month)
        sql_pubkey_guis = text(f'''
            SELECT month, pubkey, COUNT(DISTINCT gui) AS gui_count FROM (
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, maker_pubkey AS pubkey, maker_gui AS gui
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND maker_pubkey IS NOT NULL AND maker_pubkey != 'unknown' AND maker_gui IS NOT NULL AND maker_gui != 'unknown'
                UNION ALL
                SELECT TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month, taker_pubkey AS pubkey, taker_gui AS gui
                FROM defi_swaps
                WHERE finished_at >= :start_ts AND finished_at < :end_ts {pubkey_filter} {gui_filter} AND taker_pubkey IS NOT NULL AND taker_pubkey != 'unknown' AND taker_gui IS NOT NULL AND taker_gui != 'unknown'
            ) t
            GROUP BY month, pubkey
        ''')
        with query.engine.connect() as conn:
            # 1. Total swaps
            swaps_rows = conn.execute(sql_swaps, params).fetchall()
            swaps_by_month = {row.month.lower(): int(row.swap_count) for row in swaps_rows}
            # 2. Top pairs
            pairs_rows = conn.execute(sql_pairs, params).fetchall()
            pairs_by_month = {}
            for row in pairs_rows:
                m = row.month.lower()
                if m not in pairs_by_month:
                    pairs_by_month[m] = []
                pairs_by_month[m].append({
                    'pair': row.pair_std,
                    'swap_count': int(row.swap_count),
                    'volume': float(row.volume or 0)
                })
            # 3. Unique pubkeys
            pubkeys_rows = conn.execute(sql_pubkeys, params).fetchall()
            pubkeys_by_month = {row.month.lower(): int(row.unique_pubkeys) for row in pubkeys_rows}
            # 4. Top pubkeys
            top_pubkeys_rows = conn.execute(sql_top_pubkeys, params).fetchall()
            top_pubkeys_by_month = {}
            for row in top_pubkeys_rows:
                m = row.month.lower()
                if m not in top_pubkeys_by_month:
                    top_pubkeys_by_month[m] = []
                top_pubkeys_by_month[m].append({
                    'pubkey': row.pubkey,
                    'swap_count': int(row.swap_count),
                    'volume': float(row.volume or 0)
                })
            # 5. GUI stats
            gui_stats_rows = conn.execute(sql_gui_stats, params).fetchall()
            gui_stats_by_month = {}
            for row in gui_stats_rows:
                m = row.month.lower()
                if m not in gui_stats_by_month:
                    gui_stats_by_month[m] = []
                gui_stats_by_month[m].append({
                    'gui': row.gui,
                    'swap_count': int(row.swap_count),
                    'pubkey_count': int(row.pubkey_count)
                })
            # 6. Pubkey GUI counts
            pubkey_guis_rows = conn.execute(sql_pubkey_guis, params).fetchall()
            pubkey_guis_by_month = {}
            for row in pubkey_guis_rows:
                m = row.month.lower()
                if m not in pubkey_guis_by_month:
                    pubkey_guis_by_month[m] = []
                pubkey_guis_by_month[m].append(int(row.gui_count))
        # Format response
        months = []
        for i in range(1, 13):
            m_str = datetime(year, i, 1).strftime('%b').lower()
            # Top 5 pairs
            top_pairs = [MonthlyPairStats(**p) for p in sorted(pairs_by_month.get(m_str, []), key=lambda x: x['swap_count'], reverse=True)[:5]]
            # Top 10 pubkeys
            top_pubkeys = [MonthlyPubkeyStats(**p) for p in sorted(top_pubkeys_by_month.get(m_str, []), key=lambda x: x['swap_count'], reverse=True)[:10]]
            # GUI stats
            gui_stats = [MonthlyGuiStats(**g) for g in gui_stats_by_month.get(m_str, [])]
            # Pubkey GUI count buckets
            gui_count_buckets = {1: 0, 2: 0, 3: 0, '3+': 0}
            for n in pubkey_guis_by_month.get(m_str, []):
                if n == 1:
                    gui_count_buckets[1] += 1
                elif n == 2:
                    gui_count_buckets[2] += 1
                elif n == 3:
                    gui_count_buckets[3] += 1
                elif n > 3:
                    gui_count_buckets['3+'] += 1
            months.append(MonthlyStatsItem(
                month=i,
                total_swaps=swaps_by_month.get(m_str, 0),
                top_pairs=top_pairs,
                unique_pubkeys=pubkeys_by_month.get(m_str, 0),
                top_pubkeys=top_pubkeys,
                gui_stats=gui_stats,
                pubkey_gui_counts=gui_count_buckets
            ))
        resp = MonthlyStatsResponse(
            year=year,
            months=months
        )
        return resp
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/pubkeys_by_month",
    description="Returns a dict of {year: {month: unique pubkey count}} for the given year.",
    status_code=200,
)
def pubkeys_by_month(year: int = Query(..., description="Year to aggregate (e.g. 2022)")):
    try:
        query = db.SqlQuery()
        from sqlalchemy import text
        # Use raw SQL for performance
        sql = text('''
            SELECT
                EXTRACT(YEAR FROM TO_TIMESTAMP(finished_at)) AS year,
                TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month,
                maker_pubkey AS pubkey
            FROM defi_swaps
            WHERE finished_at >= :start_ts AND finished_at < :end_ts AND maker_pubkey IS NOT NULL AND maker_pubkey != 'unknown'
            UNION ALL
            SELECT
                EXTRACT(YEAR FROM TO_TIMESTAMP(finished_at)) AS year,
                TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month,
                taker_pubkey AS pubkey
            FROM defi_swaps
            WHERE finished_at >= :start_ts AND finished_at < :end_ts AND taker_pubkey IS NOT NULL AND taker_pubkey != 'unknown'
        ''')
        from datetime import datetime
        start_dt = datetime(year, 1, 1)
        end_dt = datetime(year + 1, 1, 1)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        with query.engine.connect() as conn:
            rows = conn.execute(sql, {"start_ts": start_ts, "end_ts": end_ts}).fetchall()
        # Aggregate in Python
        result = {}
        for row in rows:
            y = int(row.year)
            m = row.month.lower()
            pk = row.pubkey
            if y not in result:
                result[y] = {}
            if m not in result[y]:
                result[y][m] = set()
            result[y][m].add(pk)
        # Convert sets to counts
        out = {}
        for y in result:
            out[y] = {m: len(result[y][m]) for m in sorted(result[y], key=lambda x: datetime.strptime(x, '%b').month)}
        return out
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)


@router.get(
    "/swaps_by_month",
    description="Returns a dict of {year: {month: swap count}} for the given year.",
    status_code=200,
)
def swaps_by_month(year: int = Query(..., description="Year to aggregate (e.g. 2022)")):
    try:
        query = db.SqlQuery()
        from sqlalchemy import text
        sql = text('''
            SELECT
                EXTRACT(YEAR FROM TO_TIMESTAMP(finished_at)) AS year,
                TO_CHAR(TO_TIMESTAMP(finished_at), 'Mon') AS month,
                COUNT(*) AS swap_count
            FROM defi_swaps
            WHERE finished_at >= :start_ts AND finished_at < :end_ts
            GROUP BY year, month
        ''')
        from datetime import datetime
        start_dt = datetime(year, 1, 1)
        end_dt = datetime(year + 1, 1, 1)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        with query.engine.connect() as conn:
            rows = conn.execute(sql, {"start_ts": start_ts, "end_ts": end_ts}).fetchall()
        result = {}
        for row in rows:
            y = int(row.year)
            m = row.month.lower()
            c = int(row.swap_count)
            if y not in result:
                result[y] = {}
            result[y][m] = c
        # Sort months
        out = {}
        for y in result:
            out[y] = {m: result[y][m] for m in sorted(result[y], key=lambda x: datetime.strptime(x, '%b').month)}
        return out
    except Exception as e:
        err = {"error": f"{e}"}
        logger.warning(err)
        return JSONResponse(status_code=400, content=err)
