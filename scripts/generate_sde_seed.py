"""
Generate data/sde_seed.json.gz from the Fuzzwork SDE CSV dumps.

Run this once locally before your first deploy, and again whenever EVE patches
push a new SDE (roughly every 6-8 weeks).  The file is committed to git so
Render can load it on cold start without hitting the network.

Usage:
    python scripts/generate_sde_seed.py
"""

import bz2
import csv
import gzip
import io
import json
import os
import sys
import time

import requests

FUZZWORK_SDE = "https://www.fuzzwork.co.uk/dump/latest/"
MANUFACTURING_ACTIVITY_ID = 1
SEED_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sde_seed.json.gz")


def fetch_csv(table: str):
    url = f"{FUZZWORK_SDE}{table}.csv.bz2"
    print(f"  Fetching {url} …")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw = bz2.decompress(r.content).decode("utf-8")
    yield from csv.DictReader(io.StringIO(raw))


def main():
    print("Downloading SDE tables from Fuzzwork…")

    bp_products = []
    for r in fetch_csv("industryActivityProducts"):
        if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
            try:
                bp_products.append([int(r["typeID"]), int(r["productTypeID"]), int(r.get("quantity", 1) or 1)])
            except (ValueError, KeyError):
                pass

    bp_materials = []
    for r in fetch_csv("industryActivityMaterials"):
        if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
            try:
                bp_materials.append([int(r["typeID"]), int(r["materialTypeID"]), int(r["quantity"])])
            except (ValueError, KeyError):
                pass

    bp_duration = []
    for r in fetch_csv("industryActivity"):
        if r.get("activityID") == str(MANUFACTURING_ACTIVITY_ID):
            secs = r.get("time") or r.get("duration") or "0"
            try:
                bp_duration.append([int(r["typeID"]), int(secs)])
            except (ValueError, KeyError):
                pass

    categories = {}
    for r in fetch_csv("invCategories"):
        try:
            categories[int(r["categoryID"])] = r.get("categoryName", "")
        except (ValueError, KeyError):
            pass

    groups = {}
    for r in fetch_csv("invGroups"):
        try:
            cat_id = int(r.get("categoryID", 0) or 0)
            groups[int(r["groupID"])] = {
                "name": r.get("groupName", ""),
                "category_id": cat_id,
                "category_name": categories.get(cat_id, ""),
            }
        except (ValueError, KeyError):
            pass

    meta_groups = {}
    for r in fetch_csv("invMetaTypes"):
        try:
            meta_groups[int(r["typeID"])] = int(r["metaGroupID"])
        except (ValueError, KeyError):
            pass

    type_info = []
    for r in fetch_csv("invTypes"):
        try:
            type_id = int(r["typeID"])
            group_id = int(r.get("groupID", 0) or 0)
            gdata = groups.get(group_id, {})
            type_info.append([
                type_id,
                r.get("typeName", f"Type {type_id}"),
                group_id,
                gdata.get("name", ""),
                gdata.get("category_id", 0),
                gdata.get("category_name", ""),
                meta_groups.get(type_id, 0),
                0,
                int(r.get("portionSize", 1) or 1),
            ])
        except (ValueError, KeyError):
            pass

    seed = {
        "generated_at": int(time.time()),
        "bp_products":  bp_products,
        "bp_materials": bp_materials,
        "bp_duration":  bp_duration,
        "type_info":    type_info,
    }

    os.makedirs(os.path.dirname(SEED_PATH), exist_ok=True)
    with gzip.open(SEED_PATH, "wt", encoding="utf-8") as f:
        json.dump(seed, f)

    size_kb = os.path.getsize(SEED_PATH) // 1024
    print(f"\nSaved {SEED_PATH} ({size_kb} KB)")
    print(f"  {len(type_info):,} types | {len(bp_products):,} blueprints | {len(bp_materials):,} materials")
    print("\nCommit data/sde_seed.json.gz to git to enable fast cold starts on Render.")


if __name__ == "__main__":
    sys.exit(main())
