#!/usr/bin/env python3
"""
YPOAI statewide extract collector: one run per state, zero API calls to public map servers.

Replaces the per-cell Overpass worker. Downloads the state's OpenStreetMap extract from Geofabrik
(published daily for bulk use), reads every business-like place once, matches it against the same
category selectors the lane worker used, assigns each place to its county with Census county
boundaries (point in polygon), and records the rows in Supabase in bulk through the existing
durable dedupe path. Then it closes every cell of the state in one call.

No model calls. Data never lands on the machine except the temporary map file, which is deleted.

Usage:
  python scripts/state_extract.py --state DE --dry-run          # count only, writes nothing
  python scripts/state_extract.py --state DE                    # full run
  python scripts/state_extract.py --state DE --pbf path.osm.pbf --counties path.zip   # use local files

Environment: SUPABASE_SERVICE_ROLE_KEY (required unless --dry-run), SUPABASE_URL (optional)
Optional: the `osmium` command-line tool; when present the extract is pre-filtered first (much faster
for large states).
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(__file__))
import worker  # noqa: E402  (reuse selectors, normalizer, Supabase client)

GEOFABRIK = "https://download.geofabrik.de/north-america/us/{slug}-latest.osm.pbf"
COUNTIES_URL = "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_county_500k.zip"  # 2021: legacy CT counties
SOURCE_NAME = "OpenStreetMap Geofabrik state extract"
COMPONENT = "gh-worker"
CHUNK_ROWS = 1500
FILTER_KEYS = ("amenity", "shop", "craft", "office", "leisure", "healthcare", "sport")

STATES = {
    "AL": ("01", "alabama"), "AK": ("02", "alaska"), "AZ": ("04", "arizona"), "AR": ("05", "arkansas"),
    "CA": ("06", "california"), "CO": ("08", "colorado"), "CT": ("09", "connecticut"), "DE": ("10", "delaware"),
    "DC": ("11", "district-of-columbia"), "FL": ("12", "florida"), "GA": ("13", "georgia"), "HI": ("15", "hawaii"),
    "ID": ("16", "idaho"), "IL": ("17", "illinois"), "IN": ("18", "indiana"), "IA": ("19", "iowa"),
    "KS": ("20", "kansas"), "KY": ("21", "kentucky"), "LA": ("22", "louisiana"), "ME": ("23", "maine"),
    "MD": ("24", "maryland"), "MA": ("25", "massachusetts"), "MI": ("26", "michigan"), "MN": ("27", "minnesota"),
    "MS": ("28", "mississippi"), "MO": ("29", "missouri"), "MT": ("30", "montana"), "NE": ("31", "nebraska"),
    "NV": ("32", "nevada"), "NH": ("33", "new-hampshire"), "NJ": ("34", "new-jersey"), "NM": ("35", "new-mexico"),
    "NY": ("36", "new-york"), "NC": ("37", "north-carolina"), "ND": ("38", "north-dakota"), "OH": ("39", "ohio"),
    "OK": ("40", "oklahoma"), "OR": ("41", "oregon"), "PA": ("42", "pennsylvania"), "RI": ("44", "rhode-island"),
    "SC": ("45", "south-carolina"), "SD": ("46", "south-dakota"), "TN": ("47", "tennessee"), "TX": ("48", "texas"),
    "UT": ("49", "utah"), "VT": ("50", "vermont"), "VA": ("51", "virginia"), "WA": ("53", "washington"),
    "WV": ("54", "west-virginia"), "WI": ("55", "wisconsin"), "WY": ("56", "wyoming"),
}

SEL_RE = re.compile(r'\["([^"]+)"(?:(=|~)"((?:[^"\\]|\\.)*)")?\]')


def log(event: str, **fields: Any) -> None:
    print(json.dumps({"t": datetime.now(timezone.utc).isoformat(), "event": event, **fields}, ensure_ascii=False), flush=True)


# --- selectors -> Python predicates ------------------------------------------------------------

def compile_selector(sel: str):
    parts = SEL_RE.findall(sel)
    if not parts or "".join(f'["{k}"' + (f'{op}"{v}"' if op else "") + "]" for k, op, v in parts) != sel:
        raise ValueError(f"unparseable selector: {sel}")
    checks = []
    for key, op, val in parts:
        if op == "=":
            checks.append(lambda t, k=key, v=val: t.get(k) == v)
        elif op == "~":
            rx = re.compile(val)
            checks.append(lambda t, k=key, r=rx: k in t and r.search(t[k]) is not None)
        else:
            checks.append(lambda t, k=key: k in t)
    return lambda tags: all(c(tags) for c in checks)


COMPILED = {slug: [compile_selector(s) for s in sels] for slug, sels in worker.CATEGORY_SELECTORS.items()}


def categories_for(tags: dict[str, str]) -> list[str]:
    return [slug for slug, preds in COMPILED.items() if any(p(tags) for p in preds)]


# --- counties ----------------------------------------------------------------------------------

def load_counties(zip_path: str, state_fips: str):
    import shapefile  # pyshp
    from shapely.geometry import shape
    from shapely.strtree import STRtree
    z = zipfile.ZipFile(zip_path)
    base = "cb_2021_us_county_500k"
    r = shapefile.Reader(shp=io.BytesIO(z.read(base + ".shp")), shx=io.BytesIO(z.read(base + ".shx")),
                         dbf=io.BytesIO(z.read(base + ".dbf")))
    fields = [f[0] for f in r.fields[1:]]
    geoms, names = [], []
    for sr in r.iterShapeRecords():
        rec = dict(zip(fields, sr.record))
        if rec["STATEFP"] != state_fips:
            continue
        geoms.append(shape(sr.shape.__geo_interface__))
        names.append(rec["NAMELSAD"] if state_fips != "11" else "District of Columbia")
    if not geoms:
        raise SystemExit(f"no counties found for FIPS {state_fips}")
    return STRtree(geoms), geoms, names


def county_of(tree, geoms, names, lon: float, lat: float) -> str | None:
    from shapely.geometry import Point
    p = Point(lon, lat)
    for i in tree.query(p):
        if geoms[i].covers(p):
            return names[i]
    return None


# --- reading the extract -----------------------------------------------------------------------

def read_places(pbf_path: str) -> list[dict[str, Any]]:
    import osmium

    places: list[dict[str, Any]] = []

    def keep(tags) -> dict[str, str] | None:
        if "name" not in tags and "brand" not in tags:
            return None
        if not any(k in tags for k in FILTER_KEYS):
            return None
        return {t.k: t.v for t in tags}

    class H(osmium.SimpleHandler):
        def node(self, n):
            t = keep(n.tags)
            if t and n.location.valid():
                places.append({"type": "node", "id": n.id, "lat": n.location.lat, "lon": n.location.lon, "tags": t})

        def way(self, w):
            t = keep(w.tags)
            if not t:
                return
            xs, ys = [], []
            for nd in w.nodes:
                if nd.location.valid():
                    xs.append(nd.location.lon); ys.append(nd.location.lat)
            if xs:
                places.append({"type": "way", "id": w.id, "center": {"lat": sum(ys) / len(ys), "lon": sum(xs) / len(xs)}, "tags": t})

    H().apply_file(pbf_path, locations=True, idx="flex_mem")
    return places


def prefilter(pbf_path: str, workdir: str) -> str:
    exe = shutil.which("osmium")
    if not exe:
        return pbf_path
    out = os.path.join(workdir, "pois.osm.pbf")
    args = [exe, "tags-filter", pbf_path] + [f"nw/{k}" for k in FILTER_KEYS] + ["-o", out, "--overwrite"]
    subprocess.run(args, check=True, capture_output=True)
    return out


def download(url: str, dest: str) -> None:
    with requests.get(url, stream=True, timeout=600, headers={"User-Agent": worker.USER_AGENT}) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


# --- main ----------------------------------------------------------------------------------------

def run(state: str, pbf: str | None, counties_zip: str | None, dry_run: bool) -> int:
    state = state.upper()
    if state not in STATES:
        raise SystemExit(f"unknown state {state}")
    fips, slug = STATES[state]
    execution_id = f"S-{state}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    supa = None if dry_run else worker.Supa()

    def hb(status: str, **details: Any) -> None:
        if supa is None:
            return
        try:
            supa.rpc("nexa_write_heartbeat", {"p_component_name": COMPONENT, "p_execution_id": execution_id, "p_status": status,
                                              "p_target_id": None, "p_details": {"state": state, "source_family": "osm_extract", **details}})
        except Exception as exc:  # noqa: BLE001
            log("heartbeat_failed", error=str(exc)[:200])

    work = tempfile.mkdtemp(prefix=f"ypoai-{state}-")
    t0 = time.time()
    try:
        hb("started")
        if not pbf:
            pbf = os.path.join(work, f"{slug}-latest.osm.pbf")
            log("download", url=GEOFABRIK.format(slug=slug))
            download(GEOFABRIK.format(slug=slug), pbf)
        if not counties_zip:
            counties_zip = os.path.join(work, "counties.zip")
            download(COUNTIES_URL, counties_zip)
        tree, geoms, names = load_counties(counties_zip, fips)
        src = prefilter(pbf, work)
        places = read_places(src)
        log("places_read", state=state, candidates=len(places), counties=len(names), seconds=round(time.time() - t0, 1))

        rows: list[dict[str, Any]] = []
        outside = 0
        for el in places:
            cats = categories_for(el["tags"])
            if not cats:
                continue
            lat = el.get("lat") or el["center"]["lat"]
            lon = el.get("lon") or el["center"]["lon"]
            county = county_of(tree, geoms, names, lon, lat)
            if not county:
                outside += 1
                continue
            for cat in cats:
                row = worker.normalize_element(el, category=cat, state=state, county=county)
                if row:
                    row["source_name"] = SOURCE_NAME
                    row["source_lane_code"] = "osm_extract"
                    rows.append(row)
        groups: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
            groups[(r["county"], r["category_slug"])] += 1
        log("matched", state=state, rows=len(rows), groups=len(groups), outside_state=outside,
            categories=len({c for _, c in groups}), seconds=round(time.time() - t0, 1))

        if dry_run:
            top = sorted(groups.items(), key=lambda kv: -kv[1])[:8]
            log("dry_run_top_groups", groups=[f"{c} / {k}: {n}" for (c, k), n in top])
            return 0

        inserted_by_group: dict[tuple[str, str], int] = defaultdict(int)
        total_ins = total_dup = 0
        rows.sort(key=lambda r: (r["county"], r["category_slug"]))
        for i in range(0, len(rows), CHUNK_ROWS):
            chunk = rows[i:i + CHUNK_ROWS]
            for attempt in range(3):
                try:
                    res = supa.rpc("nx_record_bulk", {"p_worker_name": f"ypoai-extract-{state}", "p_state": state,
                                                      "p_source_name": SOURCE_NAME, "p_source_url": GEOFABRIK.format(slug=slug),
                                                      "p_rows": chunk, "p_payload": {"adapter": "ypoai_state_extract", "execution_id": execution_id,
                                                                                     "private_review_required": True}}) or []
                    break
                except Exception as exc:  # noqa: BLE001
                    log("record_retry", attempt=attempt + 1, error=str(exc)[:200])
                    time.sleep(10 * (attempt + 1))
            else:
                hb("error", error_fingerprint="record-failed")
                raise SystemExit("recording failed three times; stopping this state")
            for g in res:
                inserted_by_group[(g["county"], g["category_slug"])] += int(g.get("inserted_count") or 0)
                total_ins += int(g.get("inserted_count") or 0)
                total_dup += int(g.get("duplicate_count") or 0)
            hb("progress", rows_collected=0, recorded=min(i + CHUNK_ROWS, len(rows)), of=len(rows))

        hits = [{"county": c, "category_slug": k, "inserted": inserted_by_group.get((c, k), 0)} for (c, k) in groups]
        closed = supa.rpc("nx_close_state_cells", {"p_state": state, "p_hits": hits,
                                                    "p_mapped": sorted(worker.CATEGORY_SELECTORS), "p_execution_id": execution_id})
        log("state_done", state=state, inserted=total_ins, duplicates=total_dup, cells=closed, seconds=round(time.time() - t0, 1))
        hb("completed", rows_collected=total_ins, duplicates=total_dup, cells=closed)
        return 0
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        log("state_error", state=state, error=str(exc)[:300])
        hb("error", error_fingerprint="state-extract-error", message=str(exc)[:200])
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True)
    ap.add_argument("--pbf")
    ap.add_argument("--counties")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    return run(a.state, a.pbf, a.counties, a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
