"""
EveIndustry&Markets – Manufacturing Profitability Calculator
============================================================
Data sources:
  Fuzzwork SDE CSV dumps  – blueprint materials, activities, type/group/category info
  Fuzzwork Market API     – live Jita buy/sell order aggregates + listed volume
  CCP ESI                 – 7-day avg prices, adjusted prices, market history

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

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
DB_PATH   = os.path.join(CACHE_DIR, "eve.db")

SDE_TTL     = 86_400 * 7   # 7 days
PRICE_TTL   = 600           # 10 minutes
HISTORY_TTL = 86_400        # 24 hours (ESI market history is updated once/day)

# ── Shared state ───────────────────────────────────────────────────────────────
_init_status: dict = {"status": "loading", "message": "Starting up…", "count": 0}
_type_ids:    list = []


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
            -- Blueprint data
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
            -- Type info (products and materials)
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
            -- Market prices (current orders)
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
            -- Market history for saturation calculation
            CREATE TABLE IF NOT EXISTS market_history (
                type_id       INTEGER PRIMARY KEY,
                avg_daily_vol REAL    DEFAULT 0,
                updated_at    INTEGER DEFAULT 0
            );
        """)
        # Migration: add category_name if upgrading from older schema
        try:
            conn.execute("ALTER TABLE type_info ADD COLUMN category_name TEXT DEFAULT ''")
        except Exception:
            pass  # column already exists


# ── SDE helpers ────────────────────────────────────────────────────────────────
def _fetch_sde_csv(table: str):
    """Download and decompress a Fuzzwork SDE CSV, yielding one dict per row."""
    url = f"{FUZZWORK_SDE}{table}.csv.bz2"
    log.info("Fetching SDE: %s", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw = bz2.decompress(r.content).decode("utf-8")
    yield from csv.DictReader(io.StringIO(raw))


def load_sde_data():
    """
    Download and cache all SDE data: blueprints + full type/group/category info.
    Type info is sourced from SDE CSVs rather than ESI to avoid thousands of
    individual API calls and to get complete coverage of all item types.
    """
    with db() as conn:
        row = conn.execute("SELECT updated_at FROM meta WHERE key='sde'").fetchone()
    if row and (time.time() - row["updated_at"]) < SDE_TTL:
        log.info("SDE data fresh – skipping download.")
        return

    _set_status("loading", "Downloading SDE blueprint tables (one-time, ~60 s)…")

    # ── Blueprint products ──────────────────────────────────────────────────────
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

    # ── Blueprint materials ─────────────────────────────────────────────────────
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

    # ── Blueprint durations ─────────────────────────────────────────────────────
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

    # ── Type information from SDE ───────────────────────────────────────────────
    _set_status("loading", "Loading type information from SDE…")

    # Categories: categoryID -> name
    categories: dict = {}
    for r in _fetch_sde_csv("invCategories"):
        try:
            categories[int(r["categoryID"])] = r.get("categoryName", "")
        except (ValueError, KeyError):
            pass

    # Groups: groupID -> {name, category_id, category_name}
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

    # Meta groups: typeID -> metaGroupID (2 = Tech II)
    meta_groups: dict = {}
    for r in _fetch_sde_csv("invMetaTypes"):
        try:
            meta_groups[int(r["typeID"])] = int(r["metaGroupID"])
        except (ValueError, KeyError):
            pass

    # Types: stream directly into DB row-by-row to avoid large RAM spikes
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
                        0,  # meta_level not available directly from invTypes
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
    """Return all type IDs that are Tech II (metaGroupID=2) and have manufacturing blueprints."""
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

    # Fuzzwork aggregated orders (batches of 200)
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

    # ESI average / adjusted prices (single endpoint, full universe)
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
    """
    Fetch ESI daily market history for each type and compute the 7-day avg
    daily traded volume.  Cache for 24 h since ESI history is updated once/day.
    """
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

    log.info("Fetching ESI market history for %d types (saturation calc)…", len(type_ids))
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
    """
    Return one dict per manufacturable T2 item using batched DB queries.
    All price data is raw – profit calculations happen in JavaScript.
    """
    if not type_ids:
        return []

    CHUNK = 900  # stay well under SQLite's 999 parameter limit

    with db() as conn:
        # All blueprints for the given product IDs
        bp_rows = conn.execute(
            f"SELECT bp_id, product_id, qty FROM bp_products "
            f"WHERE product_id IN ({','.join('?'*len(type_ids))})",
            type_ids,
        ).fetchall()

        if not bp_rows:
            return []

        bp_ids   = [r["bp_id"]     for r in bp_rows]
        prod_ids = [r["product_id"] for r in bp_rows]

        # Batch: materials for all blueprints
        mats_by_bp: dict = {}
        for i in range(0, len(bp_ids), CHUNK):
            chunk = bp_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT bp_id, mat_id, qty FROM bp_materials "
                f"WHERE bp_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mats_by_bp.setdefault(r["bp_id"], []).append(r)

        # Batch: durations
        dur_by_bp: dict = {}
        for i in range(0, len(bp_ids), CHUNK):
            chunk = bp_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT bp_id, secs FROM bp_duration "
                f"WHERE bp_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                dur_by_bp[r["bp_id"]] = r["secs"]

        # Batch: type_info for products
        ti_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM type_info WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                ti_by_id[r["type_id"]] = dict(r)

        # Collect all unique material IDs
        all_mat_ids = list({m["mat_id"] for mats in mats_by_bp.values() for m in mats})

        # Batch: type_info for materials (names only)
        mat_name_by_id: dict = {}
        for i in range(0, len(all_mat_ids), CHUNK):
            chunk = all_mat_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT type_id, name FROM type_info WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mat_name_by_id[r["type_id"]] = r["name"]

        # Batch: market prices for products
        mp_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM market_prices WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mp_by_id[r["type_id"]] = dict(r)

        # Batch: market prices for materials
        mat_mp_by_id: dict = {}
        for i in range(0, len(all_mat_ids), CHUNK):
            chunk = all_mat_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT * FROM market_prices WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                mat_mp_by_id[r["type_id"]] = dict(r)

        # Batch: market history for products (saturation)
        hist_by_id: dict = {}
        for i in range(0, len(prod_ids), CHUNK):
            chunk = prod_ids[i : i + CHUNK]
            for r in conn.execute(
                f"SELECT type_id, avg_daily_vol FROM market_history "
                f"WHERE type_id IN ({','.join('?'*len(chunk))})", chunk
            ).fetchall():
                hist_by_id[r["type_id"]] = r["avg_daily_vol"]

    # ── Assemble results ────────────────────────────────────────────────────────
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
            "tech":          ti["meta_group"],
            "meta":          ti["meta_level"],
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

        # SDE: blueprints + full type/group/category data from CSV dumps
        load_sde_data()

        # All T2 items with blueprints — pure DB query, no ESI calls needed
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
    """Start background init exactly once, regardless of which request arrives first."""
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=False)
