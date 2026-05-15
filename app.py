"""
EveIndustry&Markets – Manufacturing Profitability Calculator
============================================================
Data sources:
  Fuzzwork SDE CSV dumps  – blueprint materials, activities, type/group/category info
  Fuzzwork Market API     – live Jita buy/sell order aggregates + listed volume
  CCP ESI                 – 7-day avg prices, adjusted prices, market history
  Google Gemini API       – AI market analysis and predictions (analysis page)

All profit calculations are performed client-side in JavaScript so that the
user can adjust ME%, system cost index, broker fee, and sales tax in real time
without re-hitting the server.  This endpoint returns the raw ingredient of
those calculations for each manufacturable item.

Profit formula assumptions (enforced in app.js):
  "Buy" scenario  – sell product to best buy order  (sales tax only, no broker fee)
                  – buy materials from cheapest sell orders (no buyer fees)
  "Opt" scenario  – sell product via own sell order (broker fee + sales tax)
                  – buy materials via own buy orders (broker fee on order value)
  Manufacturing tax = sum(material.adjusted_price * eff_qty) * SCI_rate
  Market saturation % = current sell-order volume / 7-day avg daily traded volume * 100
"""

import bz2
import csv
import io
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
JITA_REGION_ID            = 10000002
MANUFACTURING_ACTIVITY_ID = 1

FUZZWORK_SDE = "https://www.fuzzwork.co.uk/dump/latest/"
FUZZWORK_MKT = "https://market.fuzzwork.co.uk/aggregates/"
ESI_BASE     = "https://esi.evetech.net/latest/"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash-lite:generateContent"
)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
DB_PATH   = os.path.join(CACHE_DIR, "eve.db")

SDE_TTL      = 86_400 * 7   # 7 days
PRICE_TTL    = 600           # 10 minutes
HISTORY_TTL  = 86_400        # 24 hours
ANALYSIS_TTL = 43_200        # 12 hours

# ── Shared state ───────────────────────────────────────────────────────────────
_init_status:     dict = {"status": "loading", "message": "Starting up…", "count": 0}
_type_ids:        list = []
_analysis_status: dict = {"status": "idle",    "message": "", "result": None}
_analysis_running: bool = False
_analysis_lock    = threading.Lock()


# ── Database ───────────────────────────────────────────────────────────────────
@contextmanager
def db():
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                key        TEXT PRIMARY KEY,
                updated_at INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS bp_products (
                bp_id      INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                qty        INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (bp_id, product_id)
            );
            CREATE TABLE IF NOT EXISTS bp_materials (
                bp_id  INTEGER NOT NULL,
                mat_id INTEGER NOT NULL,
                qty    INTEGER NOT NULL,
                PRIMARY KEY (bp_id, mat_id)
            );
            CREATE TABLE IF NOT EXISTS bp_duration (
                bp_id INTEGER PRIMARY KEY,
                secs  INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS type_info (
                type_id       INTEGER PRIMARY KEY,
                name          TEXT    DEFAULT '',
                group_id      INTEGER DEFAULT 0,
                group_name    TEXT    DEFAULT '',
                category_id   INTEGER DEFAULT 0,
                category_name TEXT    DEFAULT '',
                meta_group    INTEGER DEFAULT 0,
                meta_level    INTEGER DEFAULT 0,
                portion_size  INTEGER DEFAULT 1,
                updated_at    INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS market_prices (
                type_id        INTEGER PRIMARY KEY,
                buy_max        REAL DEFAULT 0,
                buy_pct        REAL DEFAULT 0,
                sell_min       REAL DEFAULT 0,
                sell_pct       REAL DEFAULT 0,
                sell_volume    REAL DEFAULT 0,
                avg_price      REAL DEFAULT 0,
                adjusted_price REAL DEFAULT 0,
                updated_at     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS market_history (
                type_id       INTEGER PRIMARY KEY,
                avg_daily_vol REAL    DEFAULT 0,
                updated_at    INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS analysis_cache (
                id         INTEGER PRIMARY KEY,
                data       TEXT,
                updated_at INTEGER DEFAULT 0
            );
        """)
        try:
            conn.execute("ALTER TABLE type_info ADD COLUMN category_name TEXT DEFAULT ''")
        except Exception:
            pass


# ── SDE helpers ────────────────────────────────────────────────────────────────
def _fetch_sde_csv(table: str):
    url = f"{FUZZWORK_SDE}{table}.csv.bz2"
    log.info("Fetching SDE: %s", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw = bz2.decompress(r.content).decode("utf-8")
    yield from csv.DictReader(io.StringIO(raw))


def load_sde_data():
    with db() as conn:
        row = conn.execute("SELECT updated_at FROM meta WHERE key='sde'").fetchone()
    if row and (time.time() - row["updated_at"]) < SDE_TTL:
        log.info("SDE data fresh – skipping download.")
        return

    _set_status("loading", "Downloading SDE blueprint tables (one-time, ~60 s)…")

    with db() as conn:
        conn.execute("DELETE FROM bp_products")
        for r in _fetch_sde_csv("industryActivityProducts"):
            if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO bp_products VALUES (?,?,?)",
                        (int(r["typeID"]), int(r["productTypeID"]), int(r.get("quantity", 1) or 1)),
                    )
                except (ValueError, KeyError):
                    pass

    with db() as conn:
        conn.execute("DELETE FROM bp_materials")
        for r in _fetch_sde_csv("industryActivityMaterials"):
            if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO bp_materials VALUES (?,?,?)",
                        (int(r["typeID"]), int(r["materialTypeID"]), int(r["quantity"])),
                    )
                except (ValueError, KeyError):
                    pass

    with db() as conn:
        conn.execute("DELETE FROM bp_duration")
        for r in _fetch_sde_csv("industryActivity"):
            if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
                secs = r.get("time") or r.get("duration") or "0"
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO bp_duration VALUES (?,?)",
                        (int(r["typeID"]), int(secs)),
                    )
                except (ValueError, KeyError):
                    pass

    _set_status("loading", "Loading type information from SDE…")

    categories: dict = {}
    for r in _fetch_sde_csv("invCategories"):
        try:
            categories[int(r["categoryID"])] = r.get("categoryName", "")
        except (ValueError, KeyError):
            pass

    groups: dict = {}
    for r in _fetch_sde_csv("invGroups"):
        try:
            cat_id = int(r.get("categoryID", 0) or 0)
            groups[int(r["groupID"])] = {
                "name":          r.get("groupName", ""),
                "category_id":   cat_id,
                "category_name": categories.get(cat_id, ""),
            }
        except (ValueError, KeyError):
            pass

    meta_groups: dict = {}
    for r in _fetch_sde_csv("invMetaTypes"):
        try:
            meta_groups[int(r["typeID"])] = int(r["metaGroupID"])
        except (ValueError, KeyError):
            pass

    with db() as conn:
        conn.execute("DELETE FROM type_info")
        now = int(time.time())
        for r in _fetch_sde_csv("invTypes"):
            try:
                type_id  = int(r["typeID"])
                group_id = int(r.get("groupID", 0) or 0)
                gdata    = groups.get(group_id, {})
                conn.execute(
                    """INSERT OR REPLACE INTO type_info
                       (type_id, name, group_id, group_name, category_id, category_name,
                        meta_group, meta_level, portion_size, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        type_id,
                        r.get("typeName", f"Type {type_id}"),
                        group_id,
                        gdata.get("name", ""),
                        gdata.get("category_id", 0),
                        gdata.get("category_name", ""),
                        meta_groups.get(type_id, 0),
                        0,
                        int(r.get("portionSize", 1) or 1),
                        now,
                    ),
                )
            except (ValueError, KeyError):
                pass

    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('sde', ?)", (int(time.time()),))

    log.info("SDE data loaded.")


def get_t2_manufacturable_type_ids() -> list:
    with db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT bp.product_id
            FROM bp_products bp
            INNER JOIN type_info ti ON bp.product_id = ti.type_id
            WHERE ti.meta_group = 2
        """).fetchall()
    result = [r[0] for r in rows]
    log.info("Found %d T2 manufacturable type IDs", len(result))
    return result


# ── Market prices ──────────────────────────────────────────────────────────────
def refresh_market_prices(type_ids: list, force: bool = False):
    if not type_ids:
        return

    if not force:
        with db() as conn:
            oldest = conn.execute(
                f"SELECT MIN(updated_at) FROM market_prices "
                f"WHERE type_id IN ({','.join('?'*len(type_ids))})",
                type_ids,
            ).fetchone()[0] or 0
        if (time.time() - oldest) < PRICE_TTL:
            return

    log.info("Refreshing market prices for %d types…", len(type_ids))

    fuzz_data: dict = {}
    for i in range(0, len(type_ids), 200):
        batch = type_ids[i : i + 200]
        try:
            r = requests.get(
                FUZZWORK_MKT,
                params={"region": JITA_REGION_ID, "types": ",".join(str(t) for t in batch)},
                timeout=30,
            )
            r.raise_for_status()
            fuzz_data.update(r.json())
        except Exception as exc:
            log.error("Fuzzwork market fetch failed: %s", exc)

    esi_avg: dict = {}
    try:
        r = requests.get(f"{ESI_BASE}markets/prices/", params={"datasource": "tranquility"}, timeout=30)
        r.raise_for_status()
        for p in r.json():
            esi_avg[p["type_id"]] = p
    except Exception as exc:
        log.error("ESI prices fetch failed: %s", exc)

    now = int(time.time())
    with db() as conn:
        for tid in type_ids:
            fuzz = fuzz_data.get(str(tid), {})
            esi  = esi_avg.get(tid, {})
            buy  = fuzz.get("buy", {})
            sell = fuzz.get("sell", {})
            conn.execute(
                """INSERT OR REPLACE INTO market_prices
                   (type_id, buy_max, buy_pct, sell_min, sell_pct, sell_volume,
                    avg_price, adjusted_price, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    tid,
                    float(buy.get("max")        or 0),
                    float(buy.get("percentile") or 0),
                    float(sell.get("min")        or 0),
                    float(sell.get("percentile") or 0),
                    float(sell.get("volume")    or 0),
                    float(esi.get("average_price")  or 0),
                    float(esi.get("adjusted_price") or 0),
                    now,
                ),
            )
    log.info("Market prices refreshed.")


# ── Market history (for saturation) ───────────────────────────────────────────
def refresh_market_history(type_ids: list, force: bool = False):
    if not type_ids:
        return

    if not force:
        with db() as conn:
            oldest = conn.execute(
                f"SELECT MIN(updated_at) FROM market_history "
                f"WHERE type_id IN ({','.join('?'*len(type_ids))})",
                type_ids,
            ).fetchone()[0] or 0
        if (time.time() - oldest) < HISTORY_TTL:
            return

    log.info("Fetching ESI market history for %d types…", len(type_ids))
    now = int(time.time())

    with db() as conn:
        for tid in type_ids:
            try:
                r = requests.get(
                    f"{ESI_BASE}markets/{JITA_REGION_ID}/history/",
                    params={"type_id": tid, "datasource": "tranquility"},
                    timeout=15,
                )
                r.raise_for_status()
                history = r.json()
                recent = sorted(history, key=lambda x: x.get("date", ""))[-7:]
                avg_vol = (
                    sum(float(d.get("volume", 0)) for d in recent) / len(recent)
                    if recent else 0.0
                )
                conn.execute(
                    "INSERT OR REPLACE INTO market_history (type_id, avg_daily_vol, updated_at) VALUES (?,?,?)",
                    (tid, avg_vol, now),
                )
                time.sleep(0.05)
            except Exception as exc:
                log.warning("ESI history failed for type %d: %s", tid, exc)
                conn.execute(
                    "INSERT OR IGNORE INTO market_history (type_id, avg_daily_vol, updated_at) VALUES (?,0,?)",
                    (tid, now),
                )

    log.info("Market history refreshed.")


# ── Build raw component data for the frontend ─────────────────────────────────
def _safe_float(v) -> float:
    try:
        return float(v) if v else 0.0
    except (TypeError, ValueError):
        return 0.0


def build_component_data(type_ids: list) -> list:
    if not type_ids:
        return []

    CHUNK = 900

    with db() as conn:
        bp_rows = conn.execute(
            f"SELECT bp_id, product_id, qty FROM bp_products "
            f"WHERE product_id IN ({','.join('?'*len(type_ids))})",
            type_ids,
        ).fetchall()

        if not bp_rows:
            return []

        bp_ids   = [r["bp_id"]     for r in bp_rows]
        prod_ids = [r["product_id"] for r in bp_rows]

        mats_by_bp: dict = {}
        for i in range(0, len(bp_ids), CHUNK):
            chunk = bp_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT bp_id, mat_id, qty FROM bp_materials "
                f"WHERE bp_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mats_by_bp.setdefault(r["bp_id"], []).append(r)

        dur_by_bp: dict = {}
        for i in range(0, len(bp_ids), CHUNK):
            chunk = bp_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT bp_id, secs FROM bp_duration "
                f"WHERE bp_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                dur_by_bp[r["bp_id"]] = r["secs"]

        ti_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM type_info WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                ti_by_id[r["type_id"]] = dict(r)

        all_mat_ids = list({m["mat_id"] for mats in mats_by_bp.values() for m in mats})

        mat_name_by_id: dict = {}
        for i in range(0, len(all_mat_ids), CHUNK):
            chunk = all_mat_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT type_id, name FROM type_info WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mat_name_by_id[r["type_id"]] = r["name"]

        mp_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM market_prices WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mp_by_id[r["type_id"]] = dict(r)

        mat_mp_by_id: dict = {}
        for i in range(0, len(all_mat_ids), CHUNK):
            chunk = all_mat_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM market_prices WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mat_mp_by_id[r["type_id"]] = dict(r)

        hist_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT type_id, avg_daily_vol FROM market_history "
                f"WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                hist_by_id[r["type_id"]] = r["avg_daily_vol"]

    results = []
    for bp in bp_rows:
        bp_id    = bp["bp_id"]
        prod_id  = bp["product_id"]
        prod_qty = bp["qty"]

        mats = mats_by_bp.get(bp_id)
        if not mats:
            continue

        ti = ti_by_id.get(prod_id)
        if not ti:
            continue

        pp = mp_by_id.get(prod_id)
        if not pp:
            continue

        avg_daily_vol = _safe_float(hist_by_id.get(prod_id, 0))
        sell_vol      = _safe_float(pp.get("sell_volume", 0))
        saturation = (
            round(sell_vol / avg_daily_vol * 100, 1)
            if avg_daily_vol > 0 else None
        )

        materials_out = []
        for m in mats:
            mid = m["mat_id"]
            mp  = mat_mp_by_id.get(mid, {})
            materials_out.append({
                "type_id":        mid,
                "name":           mat_name_by_id.get(mid, f"Type {mid}"),
                "base_qty":       m["qty"],
                "sell_pct":       _safe_float(mp.get("sell_pct") or mp.get("sell_min", 0)),
                "buy_pct":        _safe_float(mp.get("buy_pct")  or mp.get("buy_max", 0)),
                "avg_price":      _safe_float(mp.get("avg_price", 0)),
                "adjusted_price": _safe_float(mp.get("adjusted_price", 0)),
            })

        results.append({
            "type_id":       prod_id,
            "name":          ti["name"],
            "group":         ti["group_name"],
            "category_id":   ti["category_id"],
            "category_name": ti.get("category_name", ""),
            "avg_daily_vol": avg_daily_vol,
            "duration_sec":  dur_by_bp.get(bp_id, 0),
            "product_qty":   prod_qty,
            "saturation":    saturation,
            "product": {
                "buy_max":        _safe_float(pp.get("buy_max", 0)),
                "sell_pct":       _safe_float(pp.get("sell_pct") or pp.get("sell_min", 0)),
                "avg_price":      _safe_float(pp.get("avg_price", 0)),
                "adjusted_price": _safe_float(pp.get("adjusted_price", 0)),
            },
            "materials": materials_out,
        })

    return results


def _collect_material_type_ids(product_type_ids: list) -> list:
    if not product_type_ids:
        return []
    CHUNK = 900
    mat_ids = set()
    with db() as conn:
        for i in range(0, len(product_type_ids), CHUNK):
            chunk = product_type_ids[i : i + CHUNK]
            rows = conn.execute(
                f"""SELECT DISTINCT bm.mat_id
                    FROM bp_materials bm
                    INNER JOIN bp_products bp ON bm.bp_id = bp.bp_id
                    WHERE bp.product_id IN ({','.join('?'*len(chunk))})""",
                chunk,
            ).fetchall()
            mat_ids.update(r[0] for r in rows)
    return list(mat_ids)


# ── AI Analysis ────────────────────────────────────────────────────────────────
def _build_analysis_items() -> list:
    """Pull all T2 items with price data into a compact list for the AI prompt."""
    with db() as conn:
        # How many T2 blueprints use each type as a material (structural demand signal)
        dep_rows = conn.execute("""
            SELECT bm.mat_id, COUNT(DISTINCT bp_outer.product_id) AS dep_count
            FROM bp_materials bm
            JOIN bp_products bp_outer ON bm.bp_id = bp_outer.bp_id
            JOIN type_info ti_outer   ON bp_outer.product_id = ti_outer.type_id
            WHERE ti_outer.meta_group = 2
            GROUP BY bm.mat_id
        """).fetchall()
        dep_counts = {r["mat_id"]: r["dep_count"] for r in dep_rows}

        rows = conn.execute("""
            SELECT
                ti.type_id, ti.name, ti.category_name, ti.group_name,
                mp.sell_pct, mp.buy_max, mp.avg_price, mp.sell_volume
            FROM type_info ti
            JOIN bp_products bpp ON ti.type_id = bpp.product_id
            JOIN market_prices mp ON ti.type_id = mp.type_id
            LEFT JOIN market_history mh ON ti.type_id = mh.type_id
            WHERE ti.meta_group = 2
              AND (mp.sell_pct > 0 OR mp.buy_max > 0)
            GROUP BY ti.type_id
            ORDER BY (COALESCE(mh.avg_daily_vol, 0) * mp.avg_price) DESC
        """).fetchall()

        hist_rows = conn.execute("""
            SELECT mh.type_id, mh.avg_daily_vol
            FROM market_history mh
            JOIN type_info ti ON mh.type_id = ti.type_id
            WHERE ti.meta_group = 2
        """).fetchall()
        hist_map = {r["type_id"]: r["avg_daily_vol"] for r in hist_rows}

    items = []
    for r in rows:
        avg_price     = float(r["avg_price"] or 0)
        sell_pct      = float(r["sell_pct"]  or 0)
        avg_daily_vol = float(hist_map.get(r["type_id"], 0))
        sell_volume   = float(r["sell_volume"] or 0)

        daily_isk_vol   = avg_daily_vol * avg_price
        price_mom_pct   = ((sell_pct - avg_price) / avg_price * 100) if avg_price > 0 else 0
        saturation_pct  = (sell_volume / avg_daily_vol * 100) if avg_daily_vol > 0 else None
        dep_count       = dep_counts.get(r["type_id"], 0)

        items.append({
            "type_id":        r["type_id"],
            "name":           r["name"],
            "category":       r["category_name"] or "Unknown",
            "group":          r["group_name"]    or "Unknown",
            "sell_price":     sell_pct,
            "avg_price":      avg_price,
            "buy_price":      float(r["buy_max"] or 0),
            "price_mom_pct":  round(price_mom_pct, 1),
            "daily_isk_vol":  daily_isk_vol,
            "saturation_pct": round(saturation_pct, 1) if saturation_pct is not None else None,
            "dep_count":      dep_count,
        })

    return items


def _fmt_compact(v: float, unit: str = "") -> str:
    """Format a large number compactly for the AI prompt."""
    if v == 0:
        return "0"
    if v >= 1e9:
        return f"{v/1e9:.1f}B{unit}"
    if v >= 1e6:
        return f"{v/1e6:.1f}M{unit}"
    if v >= 1e3:
        return f"{v/1e3:.0f}k{unit}"
    return f"{v:.0f}{unit}"


def _build_analysis_prompt(items: list) -> str:
    header = "type_id|name|category|group|sell|avg_7d|Δ_vs_7d|isk_vol_day|sat%|used_in_T2_BPs"
    rows = []
    for item in items:
        sat = f"{item['saturation_pct']:.0f}" if item["saturation_pct"] is not None else "?"
        rows.append(
            f"{item['type_id']}|{item['name']}|{item['category']}|{item['group']}|"
            f"{_fmt_compact(item['sell_price'])}|{_fmt_compact(item['avg_price'])}|"
            f"{item['price_mom_pct']:+.1f}%|{_fmt_compact(item['daily_isk_vol'])}|"
            f"{sat}%|{item['dep_count']}"
        )
    data_block = "\n".join(rows)

    return f"""You are an expert Eve Online market analyst specialising in Tech II manufacturing.

Analyse ALL of the following T2 manufacturable items and produce buy/sell ratings for each.

COLUMN GUIDE:
- sell            = current Jita sell order price (5th percentile)
- avg_7d          = ESI 7-day volume-weighted average price (universe-wide)
- Δ_vs_7d         = (sell - avg_7d) / avg_7d × 100. Negative = current price BELOW avg (potential recovery). Positive = above avg (may retrace).
- isk_vol_day     = avg_daily_vol × avg_price (market liquidity — how much ISK trades per day)
- sat%            = sell_volume / avg_daily_vol × 100. Low (<50%) = undersupplied. High (>200%) = oversupplied.
- used_in_T2_BPs  = count of other T2 blueprints that require this item as a material (structural demand).

RATING CRITERIA (apply ALL signals together):
- STRONG BUY:  sat% < 50 AND Δ_vs_7d < -5% AND isk_vol_day high → undersupplied + price dip + real demand
- GOOD BUY:    sat% < 100 AND (Δ_vs_7d < 0 OR used_in_T2_BPs >= 3) → moderate opportunity
- HOLD:        no strong signals in either direction; stable market
- AVOID:       sat% > 200 OR (Δ_vs_7d > +15% AND sat% > 120) → oversupplied or price likely to retrace

{header}
{data_block}

Return ONLY valid JSON — no markdown, no code fences — with this exact structure:
{{
  "market_summary": "3-4 sentence overall T2 market assessment covering the broadest trends visible in the data",
  "key_insights": [
    "Actionable insight 1 (cite specific items or categories)",
    "Actionable insight 2",
    "Actionable insight 3",
    "Actionable insight 4",
    "Actionable insight 5"
  ],
  "strong_buy": [
    {{
      "type_id": 12345,
      "name": "Item Name",
      "projected_upside_pct": 15,
      "confidence": "high",
      "reasoning": "1-2 sentences citing specific numbers from the data"
    }}
  ],
  "good_buy": [ same structure as strong_buy ],
  "hold": [
    {{ "type_id": 12345, "name": "Item Name", "reasoning": "Brief reason" }}
  ],
  "avoid": [
    {{ "type_id": 12345, "name": "Item Name", "reasoning": "Brief reason" }}
  ]
}}

Rate ALL items — every item must appear in exactly one of: strong_buy, good_buy, hold, avoid.
Aim for: 5-15 strong_buy, 15-30 good_buy, the rest in hold, 5-15 avoid.
Keep each reasoning under 80 words. projected_upside_pct is the expected % price recovery (strong_buy/good_buy only).
"""


def _call_gemini(prompt: str) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is not set.")

    r = requests.post(
        GEMINI_URL,
        params={"key": api_key},
        json={
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "temperature":      0.2,
                "maxOutputTokens":  8192,
            },
        },
        timeout=180,
    )
    r.raise_for_status()

    data = r.json()
    raw  = data["candidates"][0]["content"]["parts"][0]["text"]
    # Strip any accidental markdown fences
    raw  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    return json.loads(raw)


def _enrich_ratings(result: dict, items_by_id: dict) -> dict:
    """Attach current market data to each rated item for UI display."""
    for section in ("strong_buy", "good_buy", "hold", "avoid"):
        enriched = []
        for item in result.get(section, []):
            tid = item.get("type_id")
            if tid and tid in items_by_id:
                d = items_by_id[tid]
                item.update({
                    "category":       d["category"],
                    "group":          d["group"],
                    "sell_price":     d["sell_price"],
                    "avg_price":      d["avg_price"],
                    "buy_price":      d["buy_price"],
                    "price_mom_pct":  d["price_mom_pct"],
                    "daily_isk_vol":  d["daily_isk_vol"],
                    "saturation_pct": d["saturation_pct"],
                    "dep_count":      d["dep_count"],
                })
            enriched.append(item)
        result[section] = enriched
    return result


def _run_analysis():
    global _analysis_running
    try:
        _analysis_status["status"]  = "running"
        _analysis_status["message"] = "Gathering market data from database…"
        _analysis_status["result"]  = None

        items = _build_analysis_items()
        if not items:
            _analysis_status["status"]  = "error"
            _analysis_status["message"] = "No price data available yet — run the manufacturing init first."
            return

        _analysis_status["message"] = f"Sending {len(items)} items to Gemini for analysis…"
        log.info("AI analysis: sending %d items to Gemini", len(items))

        prompt = _build_analysis_prompt(items)
        result = _call_gemini(prompt)

        items_by_id = {item["type_id"]: item for item in items}
        result = _enrich_ratings(result, items_by_id)
        result["generated_at"] = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
        result["item_count"]   = len(items)

        with db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO analysis_cache (id, data, updated_at) VALUES (1, ?, ?)",
                (json.dumps(result), int(time.time())),
            )

        _analysis_status["status"]  = "ready"
        _analysis_status["message"] = "Analysis complete."
        _analysis_status["result"]  = result
        log.info("AI analysis complete.")

    except Exception as exc:
        log.exception("AI analysis failed")
        _analysis_status["status"]  = "error"
        _analysis_status["message"] = str(exc)
    finally:
        with _analysis_lock:
            _analysis_running = False


# ── Status helpers ─────────────────────────────────────────────────────────────
def _set_status(status: str, message: str = "", count: int = 0):
    _init_status["status"]  = status
    _init_status["message"] = message
    if count:
        _init_status["count"] = count


# ── Background initialisation ──────────────────────────────────────────────────
def _background_init():
    global _type_ids
    try:
        init_db()
        load_sde_data()

        _set_status("loading", "Discovering T2 manufacturable types…")
        tids = get_t2_manufacturable_type_ids()
        if not tids:
            _set_status("error", "No T2 types found. Check logs.")
            return
        _type_ids = tids

        mat_ids = _collect_material_type_ids(tids)
        all_price_ids = list(set(tids + mat_ids))

        _set_status("loading", f"Refreshing market prices for {len(all_price_ids)} types…")
        refresh_market_prices(all_price_ids)

        _set_status("loading", f"Fetching market history for {len(tids)} products…")
        refresh_market_history(tids)

        _set_status("ready", "Data loaded.", len(tids))
        log.info("Initialisation complete. %d T2 components ready.", len(tids))

    except Exception as exc:
        log.exception("Background init failed")
        _set_status("error", str(exc))


_init_started = False
_init_lock    = threading.Lock()

def ensure_init():
    global _init_started
    if _init_started:
        return
    with _init_lock:
        if not _init_started:
            _init_started = True
            threading.Thread(target=_background_init, daemon=True).start()


# ── Flask routes ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    ensure_init()
    return render_template("index.html")


@app.route("/analysis")
def analysis():
    ensure_init()
    return render_template("analysis.html")


@app.route("/api/status")
def api_status():
    ensure_init()
    return jsonify(_init_status)


@app.route("/api/components")
def api_components():
    ensure_init()
    if _init_status["status"] != "ready":
        return jsonify({"error": "Not ready", "status": _init_status}), 503
    return jsonify(build_component_data(_type_ids))


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    ensure_init()
    if not _type_ids:
        return jsonify({"error": "Not initialised yet"}), 503

    def _do():
        mat_ids = _collect_material_type_ids(_type_ids)
        refresh_market_prices(list(set(_type_ids + mat_ids)), force=True)
        refresh_market_history(_type_ids, force=True)
        _set_status("ready", "Data loaded.", len(_type_ids))

    threading.Thread(target=_do, daemon=True).start()
    return jsonify({"status": "refresh started"})


@app.route("/api/analysis/run", methods=["POST"])
def api_analysis_run():
    global _analysis_running
    ensure_init()

    # Return cached result if still fresh
    with db() as conn:
        cached = conn.execute(
            "SELECT data, updated_at FROM analysis_cache WHERE id=1"
        ).fetchone()
    if cached and (time.time() - cached["updated_at"]) < ANALYSIS_TTL:
        result = json.loads(cached["data"])
        _analysis_status["status"] = "ready"
        _analysis_status["result"] = result
        return jsonify({"status": "ready", "result": result})

    with _analysis_lock:
        if _analysis_running:
            return jsonify({"status": "running", "message": _analysis_status.get("message", "")})
        _analysis_running = True

    threading.Thread(target=_run_analysis, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/analysis/result")
def api_analysis_result():
    ensure_init()
    if _analysis_status["status"] == "ready" and _analysis_status.get("result"):
        return jsonify({"status": "ready", "result": _analysis_status["result"]})

    # Try DB cache on cold start (status reset after restart)
    with db() as conn:
        cached = conn.execute(
            "SELECT data, updated_at FROM analysis_cache WHERE id=1"
        ).fetchone()
    if cached and (time.time() - cached["updated_at"]) < ANALYSIS_TTL:
        result = json.loads(cached["data"])
        _analysis_status["status"] = "ready"
        _analysis_status["result"] = result
        return jsonify({"status": "ready", "result": result})

    return jsonify({
        "status":  _analysis_status["status"],
        "message": _analysis_status.get("message", ""),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=False)
