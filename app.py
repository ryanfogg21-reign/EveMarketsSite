"""
EveIndustry&Markets – Manufacturing Profitability Calculator
============================================================
Data sources:
  Fuzzwork SDE CSV dumps  – blueprint materials, activities, market groups
  Fuzzwork Market API     – live Jita buy/sell order aggregates + listed volume
  CCP ESI                 – type metadata, 7-day avg prices, market history

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
ADV_COMPONENTS_MKT_GROUP  = 65

FUZZWORK_SDE = "https://www.fuzzwork.co.uk/dump/latest/"
FUZZWORK_MKT = "https://market.fuzzwork.co.uk/aggregates/"
ESI_BASE     = "https://esi.evetech.net/latest/"

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
DB_PATH   = os.path.join(CACHE_DIR, "eve.db")

SDE_TTL     = 86_400 * 7   # 7 days
PRICE_TTL   = 600           # 10 minutes
HISTORY_TTL = 86_400        # 24 hours (ESI market history is daily)

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
            -- Market group child-map (JSON blob)
            CREATE TABLE IF NOT EXISTS mkt_children_blob (
                data TEXT
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
                type_id      INTEGER PRIMARY KEY,
                name         TEXT    DEFAULT '',
                group_id     INTEGER DEFAULT 0,
                group_name   TEXT    DEFAULT '',
                category_id  INTEGER DEFAULT 0,
                meta_group   INTEGER DEFAULT 0,
                meta_level   INTEGER DEFAULT 0,
                portion_size INTEGER DEFAULT 1,
                updated_at   INTEGER DEFAULT 0
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


# ── SDE helpers ────────────────────────────────────────────────────────────────
def _fetch_sde_csv(table: str):
    """
    Download and decompress a Fuzzwork SDE CSV, yielding one dict per row.
    Streams in chunks to avoid loading the entire file into RAM at once.
    """
    url = f"{FUZZWORK_SDE}{table}.csv.bz2"
    log.info("Fetching SDE: %s", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw = bz2.decompress(r.content).decode("utf-8")
    yield from csv.DictReader(io.StringIO(raw))


def load_sde_blueprints():
    with db() as conn:
        row = conn.execute("SELECT updated_at FROM meta WHERE key='sde'").fetchone()
    if row and (time.time() - row["updated_at"]) < SDE_TTL:
        log.info("SDE data fresh – skipping download.")
        return

    _set_status("loading", "Downloading SDE blueprint tables (one-time, ~30 s)…")

    # Stream each CSV directly into SQLite row-by-row — never holds the full
    # file in RAM, which keeps us well within Render's 512 MB free-tier limit.
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
        conn.execute(
            "INSERT OR REPLACE INTO meta VALUES ('sde', ?)", (int(time.time()),)
        )
    log.info("SDE blueprint data loaded.")


def _get_market_group_descendants(root_group: int) -> list:
    with db() as conn:
        row = conn.execute("SELECT updated_at FROM meta WHERE key='mkt_groups'").fetchone()
    cached_fresh = row and (time.time() - row["updated_at"]) < SDE_TTL

    children: dict = {}
    if not cached_fresh:
        _set_status("loading", "Fetching market group hierarchy…")
        rows = _fetch_sde_csv("invMarketGroups")
        for r in rows:
            try:
                gid = int(r["marketGroupID"])
                pid_raw = (r.get("parentGroupID") or "").strip()
                if pid_raw:
                    children.setdefault(int(pid_raw), []).append(gid)
            except (ValueError, KeyError):
                pass
        with db() as conn:
            conn.execute("INSERT OR REPLACE INTO meta VALUES ('mkt_groups', ?)", (int(time.time()),))
            conn.execute("DELETE FROM mkt_children_blob")
            conn.execute("INSERT INTO mkt_children_blob VALUES (?)", (json.dumps(children),))
    else:
        with db() as conn:
            row2 = conn.execute("SELECT data FROM mkt_children_blob LIMIT 1").fetchone()
        children = json.loads(row2["data"]) if row2 else {}
        # JSON keys are strings – convert to int
        children = {int(k): v for k, v in children.items()}

    result: list = []
    queue = [root_group]
    while queue:
        cur = queue.pop()
        result.append(cur)
        queue.extend(children.get(cur, []))
    return result


def get_advanced_component_type_ids() -> list:
    _set_status("loading", "Discovering Advanced Component type IDs…")
    group_ids = _get_market_group_descendants(ADV_COMPONENTS_MKT_GROUP)
    log.info("Searching %d market groups under group %d", len(group_ids), ADV_COMPONENTS_MKT_GROUP)

    type_ids: list = []
    for gid in group_ids:
        try:
            r = requests.get(
                f"{ESI_BASE}markets/groups/{gid}/",
                params={"datasource": "tranquility"},
                timeout=10,
            )
            r.raise_for_status()
            type_ids.extend(int(t) for t in r.json().get("types", []))
            time.sleep(0.1)
        except Exception as exc:
            log.warning("Failed market group %d: %s", gid, exc)

    result = list(set(type_ids))
    log.info("Discovered %d Advanced Component type IDs", len(result))
    return result


# ── Type info ──────────────────────────────────────────────────────────────────
def ensure_type_info(type_ids: list):
    if not type_ids:
        return
    with db() as conn:
        known = {
            r[0]
            for r in conn.execute(
                f"SELECT type_id FROM type_info WHERE type_id IN ({','.join('?'*len(type_ids))})",
                type_ids,
            ).fetchall()
        }
    to_fetch = [t for t in type_ids if t not in known]
    if not to_fetch:
        return

    log.info("Fetching ESI type info for %d items…", len(to_fetch))
    group_cache: dict = {}

    for type_id in to_fetch:
        try:
            r = requests.get(
                f"{ESI_BASE}universe/types/{type_id}/",
                params={"datasource": "tranquility", "language": "en"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            group_id = data.get("group_id", 0)

            if group_id not in group_cache:
                gr = requests.get(
                    f"{ESI_BASE}universe/groups/{group_id}/",
                    params={"datasource": "tranquility", "language": "en"},
                    timeout=10,
                )
                group_cache[group_id] = gr.json() if gr.ok else {}
                time.sleep(0.05)

            gdata = group_cache[group_id]
            meta_group = meta_level = 0
            for attr in data.get("dogma_attributes", []):
                aid = attr.get("attribute_id")
                if aid == 1692:
                    meta_group = int(attr["value"])
                elif aid == 633:
                    meta_level = int(attr["value"])

            with db() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO type_info
                       (type_id, name, group_id, group_name, category_id,
                        meta_group, meta_level, portion_size, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        type_id,
                        data.get("name", f"Type {type_id}"),
                        group_id,
                        gdata.get("name", ""),
                        gdata.get("category_id", 0),
                        meta_group, meta_level,
                        data.get("portion_size", 1),
                        int(time.time()),
                    ),
                )
            time.sleep(0.05)
        except Exception as exc:
            log.error("Type info failed for %d: %s", type_id, exc)


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

    # Fuzzwork aggregated orders
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

    # ESI average / adjusted prices
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
                history = r.json()   # list of {date, average, volume, …}
                # Take last 7 entries (most recent days)
                recent = sorted(history, key=lambda x: x.get("date", ""))[-7:]
                avg_vol = (
                    sum(float(d.get("volume", 0)) for d in recent) / len(recent)
                    if recent else 0.0
                )
                conn.execute(
                    "INSERT OR REPLACE INTO market_history (type_id, avg_daily_vol, updated_at) VALUES (?,?,?)",
                    (tid, avg_vol, now),
                )
                time.sleep(0.05)   # ESI rate limiting
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
    Return one dict per manufacturable advanced component.
    All price data is raw – profit calculations happen in JavaScript so that
    the user can adjust ME, SCI, broker fee, and sales tax without API round-trips.
    """
    if not type_ids:
        return []

    results = []
    with db() as conn:
        bp_rows = conn.execute(
            f"SELECT bp_id, product_id, qty FROM bp_products "
            f"WHERE product_id IN ({','.join('?'*len(type_ids))})",
            type_ids,
        ).fetchall()

        for bp in bp_rows:
            bp_id    = bp["bp_id"]
            prod_id  = bp["product_id"]
            prod_qty = bp["qty"]

            mats = conn.execute(
                "SELECT mat_id, qty FROM bp_materials WHERE bp_id = ?", (bp_id,)
            ).fetchall()
            if not mats:
                continue

            dur = conn.execute("SELECT secs FROM bp_duration WHERE bp_id = ?", (bp_id,)).fetchone()
            duration_secs = dur["secs"] if dur else 0

            ti = conn.execute("SELECT * FROM type_info WHERE type_id = ?", (prod_id,)).fetchone()
            if not ti:
                continue

            pp = conn.execute("SELECT * FROM market_prices WHERE type_id = ?", (prod_id,)).fetchone()
            if not pp:
                continue

            # Market history for saturation
            hist = conn.execute(
                "SELECT avg_daily_vol FROM market_history WHERE type_id = ?", (prod_id,)
            ).fetchone()
            avg_daily_vol = _safe_float(hist["avg_daily_vol"]) if hist else 0.0
            sell_vol      = _safe_float(pp["sell_volume"])
            saturation = (
                round(sell_vol / avg_daily_vol * 100, 1)
                if avg_daily_vol > 0 else None
            )

            # Material prices
            mat_ids = [m["mat_id"] for m in mats]
            mp_rows = conn.execute(
                f"SELECT * FROM market_prices WHERE type_id IN ({','.join('?'*len(mat_ids))})",
                mat_ids,
            ).fetchall()
            mp_map = {r["type_id"]: dict(r) for r in mp_rows}  # dict() so .get() works

            mat_name_rows = conn.execute(
                f"SELECT type_id, name FROM type_info WHERE type_id IN ({','.join('?'*len(mat_ids))})",
                mat_ids,
            ).fetchall()
            mat_names = {r["type_id"]: r["name"] for r in mat_name_rows}

            materials_out = []
            for m in mats:
                mid = m["mat_id"]
                mp  = mp_map.get(mid, {})
                materials_out.append({
                    "type_id":        mid,
                    "name":           mat_names.get(mid, f"Type {mid}"),
                    "base_qty":       m["qty"],
                    "sell_pct":       _safe_float(mp.get("sell_pct") or mp.get("sell_min") if mp else 0),
                    "buy_pct":        _safe_float(mp.get("buy_pct")  or mp.get("buy_max")  if mp else 0),
                    "avg_price":      _safe_float(mp["avg_price"]      if mp else 0),
                    "adjusted_price": _safe_float(mp["adjusted_price"] if mp else 0),
                })

            results.append({
                "type_id":      prod_id,
                "name":         ti["name"],
                "group":        ti["group_name"],
                "category_id":  ti["category_id"],
                "tech":         ti["meta_group"],
                "meta":         ti["meta_level"],
                "duration_sec": duration_secs,
                "product_qty":  prod_qty,
                "saturation":   saturation,
                "product": {
                    "buy_max":        _safe_float(pp["buy_max"]),
                    "sell_pct":       _safe_float(pp["sell_pct"] or pp["sell_min"]),
                    "avg_price":      _safe_float(pp["avg_price"]),
                    "adjusted_price": _safe_float(pp["adjusted_price"]),
                },
                "materials": materials_out,
            })

    return results


def _collect_material_type_ids(product_type_ids: list) -> list:
    if not product_type_ids:
        return []
    with db() as conn:
        rows = conn.execute(
            f"""SELECT DISTINCT bm.mat_id
                FROM bp_materials bm
                INNER JOIN bp_products bp ON bm.bp_id = bp.bp_id
                WHERE bp.product_id IN ({','.join('?'*len(product_type_ids))})""",
            product_type_ids,
        ).fetchall()
    return [r[0] for r in rows]


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
        load_sde_blueprints()

        _set_status("loading", "Discovering Advanced Component types…")
        tids = get_advanced_component_type_ids()
        if not tids:
            _set_status("error", "No Advanced Component types found. Check logs.")
            return
        _type_ids = tids

        _set_status("loading", f"Fetching metadata for {len(tids)} product types…")
        ensure_type_info(tids)

        mat_ids = _collect_material_type_ids(tids)

        if mat_ids:
            _set_status("loading", f"Fetching metadata for {len(mat_ids)} material types…")
            ensure_type_info(mat_ids)

        _set_status("loading", "Refreshing market prices…")
        refresh_market_prices(tids)
        if mat_ids:
            refresh_market_prices(mat_ids)

        _set_status("loading", "Fetching market history for saturation data…")
        refresh_market_history(tids)

        _set_status("ready", "Data loaded.", len(tids))
        log.info("Initialisation complete. %d components ready.", len(tids))

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
        refresh_market_prices(_type_ids, force=True)
        mat_ids = _collect_material_type_ids(_type_ids)
        if mat_ids:
            refresh_market_prices(mat_ids, force=True)
        refresh_market_history(_type_ids, force=True)
        _set_status("ready", "Data loaded.", len(_type_ids))

    threading.Thread(target=_do, daemon=True).start()
    return jsonify({"status": "refresh started"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port, use_reloader=False)
