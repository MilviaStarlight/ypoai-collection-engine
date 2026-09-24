#!/usr/bin/env python3
"""
YPOAI lane worker: deterministic, token-free county collection for the Yellow Pages of AI.

One process = one lane (W1..W6). It claims county x category cells from the existing
Supabase queue through the durable claim RPC (row-locked, so two workers can never take
the same cell), pulls every tagged business for that county and category from
OpenStreetMap through the Overpass API in ONE request per cell, normalizes the rows,
records them through the existing durable record RPC (which dedupes and updates the
target), and heartbeats so the n8n watchdog and the lane status view can see it.

No model calls anywhere. No files written to disk. Secrets come from the environment only.

Usage:
  python scripts/worker.py status  --lane W4
  python scripts/worker.py start   --lane W4 --hours 2 [--max-cells 200]
  python scripts/worker.py dry-run --lane W4 --max-cells 3     # Overpass only, claims nothing, writes nothing

Environment:
  SUPABASE_SERVICE_ROLE_KEY   required (a Supabase secret key, sb_secret_..., or legacy service role JWT)
  SUPABASE_URL                optional, defaults to the Agenarys project
  YPOAI_OVERPASS_URL          optional override for the Overpass endpoint
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ehzxnnxyliapvcvzzmyp.supabase.co").rstrip("/")
BI_SCHEMA = "business_intelligence"
SOURCE_NAME = "OpenStreetMap Overpass (county bulk)"
SOURCE_FAMILY = "osm_overpass"
COMPONENT_NAME = "pc-worker"
PRIVATE_REVIEW_NOTE = "Public candidate. Pending private review; not approved for public directory."
USER_AGENT = "YPOAI-collector/1.0 (+https://yellowpagesofai.com; hello@yellowpagesofai.com)"
OVERPASS_ENDPOINTS = [
    os.environ.get("YPOAI_OVERPASS_URL") or "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
OVERPASS_TIMEOUT = 90
OVERPASS_PACE_SECONDS = 2.0        # fair use: one request at a time, small gap between requests
MAX_ROWS_PER_CELL = 1500
SUPABASE_TIMEOUT = 120
COUNTY_SUFFIX_RE = re.compile(r"\s+(County|Parish|Borough|Census Area|Municipality|City and Borough|city)$", re.I)

# --- category -> Overpass selectors -------------------------------------------------------
# Each entry is a list of Overpass tag selectors; any element matching any selector inside the
# county area is a candidate. Categories with no reliable free OSM signal are absent on purpose:
# their cells are closed as 'blocked' with a reason so the free lane stops re-claiming them, and
# the licensing-board lane reopens them later.
def _sel(*parts: str) -> str:
    return "".join(f"[{p}]" for p in parts)

CATEGORY_SELECTORS: dict[str, list[str]] = {
    "accounting": [_sel('"office"="accountant"')],
    "bookkeeping": [_sel('"office"="accountant"', '"name"~"[Bb]ookkeep"')],
    "tax-preparation": [_sel('"office"="tax_advisor"'), _sel('"office"="accountant"', '"name"~"[Tt]ax"')],
    "appliance-repair": [_sel('"shop"="appliance"'), _sel('"craft"="electronics_repair"', '"name"~"[Aa]ppliance"')],
    "auto-body": [_sel('"shop"="car_repair"', '"service:vehicle:body_repair"="yes"'), _sel('"shop"="car_repair"', '"name"~"[Bb]ody|[Cc]ollision"')],
    "auto-repair": [_sel('"shop"="car_repair"')],
    "car-dealerships": [_sel('"shop"="car"')],
    "car-detailing": [_sel('"shop"="car_repair"', '"name"~"[Dd]etail"'), _sel('"amenity"="car_wash"', '"name"~"[Dd]etail"')],
    "car-wash": [_sel('"amenity"="car_wash"')],
    "tire-shops": [_sel('"shop"="tyres"')],
    "glass-repair": [_sel('"craft"="glaziery"'), _sel('"shop"="glaziery"'), _sel('"shop"="car_repair"', '"name"~"[Gg]lass"')],
    "barber": [_sel('"shop"="hairdresser"', '"name"~"[Bb]arber"')],
    "hair-salon": [_sel('"shop"="hairdresser"')],
    "nail-salon": [_sel('"shop"="beauty"', '"beauty"="nails"'), _sel('"shop"="beauty"', '"name"~"[Nn]ail"')],
    "lash-studios": [_sel('"shop"="beauty"', '"name"~"[Ll]ash"')],
    "med-spa": [_sel('"shop"="beauty"', '"name"~"[Mm]ed ?[Ss]pa|[Aa]esthetic"'), _sel('"amenity"="clinic"', '"name"~"[Mm]ed ?[Ss]pa|[Aa]esthetic"')],
    "spa": [_sel('"shop"="beauty"', '"name"~"[Ss]pa"'), _sel('"leisure"="spa"')],
    "spas": [_sel('"shop"="beauty"', '"name"~"[Ss]pa"'), _sel('"leisure"="spa"')],
    "massage-studios": [_sel('"shop"="massage"')],
    "tattoo-shops": [_sel('"shop"="tattoo"')],
    "cafes-bakeries": [_sel('"amenity"="cafe"'), _sel('"shop"="bakery"')],
    "restaurant": [_sel('"amenity"="restaurant"')],
    "food-trucks": [_sel('"amenity"="fast_food"', '"street_vendor"="yes"'), _sel('"amenity"="fast_food"', '"name"~"[Tt]ruck"')],
    "juice-bars": [_sel('"amenity"="juice_bar"'), _sel('"amenity"="cafe"', '"name"~"[Jj]uice|[Ss]moothie"')],
    "catering": [_sel('"craft"="caterer"'), _sel('"amenity"="restaurant"', '"name"~"[Cc]atering"')],
    "child-care": [_sel('"amenity"="childcare"'), _sel('"amenity"="kindergarten"')],
    "elder-care": [_sel('"amenity"="social_facility"', '"social_facility"~"assisted_living|nursing_home|group_home"')],
    "tutoring": [_sel('"office"="tutoring"'), _sel('"amenity"="language_school"')],
    "chiropractor": [_sel('"healthcare"="chiropractor"'), _sel('"amenity"="doctors"', '"healthcare:speciality"~"chiropract"'), _sel('"name"~"[Cc]hiropract"', '"amenity"~"doctors|clinic"')],
    "dentist": [_sel('"amenity"="dentist"')],
    "medical-services": [_sel('"amenity"="clinic"'), _sel('"amenity"="doctors"')],
    "urgent-care": [_sel('"amenity"="clinic"', '"name"~"[Uu]rgent"'), _sel('"healthcare"="clinic"', '"healthcare:speciality"~"urgent"')],
    "physical-therapy": [_sel('"healthcare"="physiotherapist"'), _sel('"amenity"="clinic"', '"name"~"[Pp]hysical [Tt]herapy|[Pp]hysio"')],
    "veterinarian": [_sel('"amenity"="veterinary"')],
    "pet-groomers": [_sel('"shop"="pet_grooming"')],
    "pet-boarding": [_sel('"amenity"="animal_boarding"')],
    "dog-daycare": [_sel('"amenity"="animal_boarding"', '"name"~"[Dd]aycare|[Dd]ay [Cc]are"')],
    "clothing-stores": [_sel('"shop"="clothes"')],
    "furniture-stores": [_sel('"shop"="furniture"')],
    "jewelry-stores": [_sel('"shop"="jewelry"')],
    "home-decor": [_sel('"shop"="interior_decoration"'), _sel('"shop"="houseware"')],
    "specialty-shops": [_sel('"shop"="gift"'), _sel('"shop"="craft"')],
    "flooring": [_sel('"shop"="flooring"'), _sel('"shop"="carpet"')],
    "tile-stone": [_sel('"shop"="tiles"')],
    "blinds-shades": [_sel('"shop"="curtain"')],
    "security-systems": [_sel('"shop"="security"')],
    "computer-repair": [_sel('"shop"="computer_repair"'), _sel('"craft"="electronics_repair"')],
    "phone-repair": [_sel('"shop"="mobile_phone"', '"name"~"[Rr]epair|[Ff]ix"'), _sel('"shop"="electronics_repair"')],
    "it-services": [_sel('"office"="it"')],
    "insurance-agency": [_sel('"office"="insurance"')],
    "legal-services": [_sel('"office"="lawyer"')],
    "real-estate-agent": [_sel('"office"="estate_agent"')],
    "property-management": [_sel('"office"="property_management"')],
    "business-consulting": [_sel('"office"="consulting"')],
    "moving": [_sel('"office"="moving_company"'), _sel('"shop"="moving"')],
    "funeral-home": [_sel('"shop"="funeral_directors"'), _sel('"amenity"="funeral_hall"')],
    "photography": [_sel('"craft"="photographer"'), _sel('"shop"="photo"')],
    "gyms": [_sel('"leisure"="fitness_centre"')],
    "crossfit": [_sel('"leisure"="fitness_centre"', '"name"~"[Cc]ross[Ff]it"')],
    "boxing-studios": [_sel('"leisure"="fitness_centre"', '"name"~"[Bb]ox"'), _sel('"leisure"="sports_centre"', '"sport"="boxing"')],
    "pilates": [_sel('"leisure"="fitness_centre"', '"name"~"[Pp]ilates"')],
    "yoga-studios": [_sel('"leisure"="fitness_centre"', '"name"~"[Yy]oga"'), _sel('"sport"="yoga"')],
    "handyman": [_sel('"craft"="handyman"')],
    "electrical": [_sel('"craft"="electrician"')],
    "plumbing": [_sel('"craft"="plumber"')],
    "hvac": [_sel('"craft"="hvac"')],
    "roofing": [_sel('"craft"="roofer"')],
    "painting": [_sel('"craft"="painter"')],
    "cabinetry": [_sel('"craft"="cabinet_maker"')],
    "masonry": [_sel('"craft"="stonemason"')],
    "welding": [_sel('"craft"="welder"')],
    "metal-fabrication": [_sel('"craft"="metal_construction"')],
    "insulation": [_sel('"craft"="insulation"')],
    "window-installation": [_sel('"craft"="window_construction"')],
    "landscaping": [_sel('"craft"="gardener"'), _sel('"shop"="garden_centre"', '"name"~"[Ll]andscap"')],
    "lawn-care": [_sel('"craft"="gardener"', '"name"~"[Ll]awn"')],
    "cleaning": [_sel('"craft"="cleaning"'), _sel('"shop"="dry_cleaning"')],
    "cleaning-services": [_sel('"craft"="cleaning"')],
    "carpet-cleaning": [_sel('"craft"="cleaning"', '"name"~"[Cc]arpet"')],
    "locksmith": [_sel('"craft"="locksmith"'), _sel('"shop"="locksmith"')],
    "pest-control": [_sel('"shop"="pest_control"'), _sel('"craft"="pest_control"')],
    "solar": [_sel('"craft"="electrician"', '"name"~"[Ss]olar"'), _sel('"office"="company"', '"name"~"[Ss]olar"')],
    "garage-door": [_sel('"shop"="doors"', '"name"~"[Gg]arage"'), _sel('"craft"="carpenter"', '"name"~"[Gg]arage [Dd]oor"')],
    "door-installation": [_sel('"shop"="doors"')],
    "remodeling": [_sel('"craft"="builder"'), _sel('"office"="company"', '"name"~"[Rr]emodel"')],
    "event-planning": [_sel('"office"="event_management"'), _sel('"shop"="party"')],
    "life-business-coaches": [_sel('"office"="coaching"')],
    "towing": [_sel('"shop"="car_repair"', '"name"~"[Tt]ow"'), _sel('"office"="company"', '"name"~"[Tt]owing"')],
    "roadside-assistance": [_sel('"office"="company"', '"name"~"[Rr]oadside"')],
    "junk-removal": [_sel('"office"="company"', '"name"~"[Jj]unk"')],
    "tree-service": [_sel('"craft"="gardener"', '"name"~"[Tt]ree"'), _sel('"office"="company"', '"name"~"[Tt]ree [Ss]ervice"')],
    "pool-service": [_sel('"shop"="swimming_pool"'), _sel('"office"="company"', '"name"~"[Pp]ool"')],
    "personal-trainer": [_sel('"leisure"="fitness_centre"', '"name"~"[Pp]ersonal [Tt]rain"')],
    "dog-trainers": [_sel('"amenity"="animal_training"')],
    "dog-walkers": [_sel('"office"="company"', '"name"~"[Dd]og [Ww]alk"')],
    "home-inspection": [_sel('"office"="company"', '"name"~"[Hh]ome [Ii]nspect"')],
    "water-damage-restoration": [_sel('"office"="company"', '"name"~"[Rr]estoration"')],
    "fire-damage-restoration": [_sel('"office"="company"', '"name"~"[Rr]estoration"')],
    "storm-damage-restoration": [_sel('"office"="company"', '"name"~"[Rr]estoration"')],
    "mold-remediation": [_sel('"office"="company"', '"name"~"[Mm]old"')],
    "septic": [_sel('"office"="company"', '"name"~"[Ss]eptic"')],
    "concrete": [_sel('"office"="company"', '"name"~"[Cc]oncrete"')],
    "asphalt": [_sel('"office"="company"', '"name"~"[Aa]sphalt|[Pp]aving"')],
    "driveway-paving": [_sel('"office"="company"', '"name"~"[Pp]aving"')],
    "excavation": [_sel('"office"="company"', '"name"~"[Ee]xcavat"')],
    "demolition": [_sel('"office"="company"', '"name"~"[Dd]emolition"')],
    "fencing": [_sel('"office"="company"', '"name"~"[Ff]enc"')],
    "gutter": [_sel('"office"="company"', '"name"~"[Gg]utter"')],
    "siding": [_sel('"office"="company"', '"name"~"[Ss]iding"')],
    "drywall": [_sel('"office"="company"', '"name"~"[Dd]rywall"')],
    "countertops": [_sel('"office"="company"', '"name"~"[Cc]ountertop|[Gg]ranite"')],
    "custom-closets": [_sel('"office"="company"', '"name"~"[Cc]loset"')],
    "foundation-repair": [_sel('"office"="company"', '"name"~"[Ff]oundation"')],
    "basement-waterproofing": [_sel('"office"="company"', '"name"~"[Ww]aterproof"')],
    "irrigation": [_sel('"office"="company"', '"name"~"[Ii]rrigation|[Ss]prinkler"')],
    "pressure-washing": [_sel('"office"="company"', '"name"~"[Pp]ressure [Ww]ash|[Pp]ower [Ww]ash"')],
    "radon-mitigation": [_sel('"office"="company"', '"name"~"[Rr]adon"')],
    "asbestos-abatement": [_sel('"office"="company"', '"name"~"[Aa]sbestos"')],
    "biohazard-cleanup": [_sel('"office"="company"', '"name"~"[Bb]iohazard"')],
}

# --- small helpers ------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(event: str, **fields: Any) -> None:
    print(json.dumps({"t": now_iso(), "event": event, **fields}, ensure_ascii=False), flush=True)


def norm_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def norm_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", norm_space(value).upper())


def county_has_suffix(county: str) -> bool:
    return bool(COUNTY_SUFFIX_RE.search(county))


# --- Supabase client ----------------------------------------------------------------------

class Supa:
    def __init__(self) -> None:
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not key:
            raise SystemExit("SUPABASE_SERVICE_ROLE_KEY is not set in the environment.")
        self.key = key
        self.s = requests.Session()
        self.s.headers.update({
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "User-Agent": USER_AGENT,
        })

    def rpc(self, name: str, body: dict[str, Any]) -> Any:
        r = self.s.post(f"{SUPABASE_URL}/rest/v1/rpc/{name}", json=body, timeout=SUPABASE_TIMEOUT,
                        headers={"Content-Type": "application/json"})
        if r.status_code >= 400:
            raise RuntimeError(f"rpc {name} HTTP {r.status_code}: {r.text[:300]}")
        return r.json() if r.text.strip() else None




# --- Overpass ---------------------------------------------------------------------------------

def build_overpass_query(state: str, county: str, selectors: list[str]) -> str:
    """One query: state area -> county relation -> area -> union of selectors -> elements with center + tags."""
    esc = county.replace('"', '\\"')
    if state == "DC":
        county_part = 'area["ISO3166-2"="US-DC"]->.c;'
    else:
        county_part = (
            f'area["ISO3166-2"="US-{state}"]->.st;\n'
            f'rel(area.st)["boundary"="administrative"]["admin_level"~"^(6|8)$"]["name"="{esc}"];\n'
            f'map_to_area->.c;'
        )
    union = "\n".join(f"  nwr{sel}(area.c);" for sel in selectors)
    return f"[out:json][timeout:{OVERPASS_TIMEOUT - 10}];\n{county_part}\n(\n{union}\n);\nout center tags;"


class Overpass:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": USER_AGENT})
        self.last_call = 0.0
        self.calls = 0

    def slot_wait(self, url: str) -> float:
        """Ask overpass-api.de how long until a slot frees up (per-client limit); default 15s."""
        try:
            status = self.s.get(url.rsplit("/", 1)[0] + "/status", timeout=20).text
            if "slots available now" in status and not status.startswith("0 slots"):
                m = re.search(r"(\d+) slots available now", status)
                if m and int(m.group(1)) > 0:
                    return 3.0
            m = re.search(r"in (\d+) seconds", status)
            if m:
                return min(120.0, int(m.group(1)) + 2.0)
        except Exception:  # noqa: BLE001
            pass
        return 15.0

    def query(self, q: str) -> list[dict[str, Any]]:
        wait = OVERPASS_PACE_SECONDS - (time.time() - self.last_call)
        if wait > 0:
            time.sleep(wait)
        last_err: Exception | None = None
        for attempt in range(4):
            url = OVERPASS_ENDPOINTS[attempt % len(OVERPASS_ENDPOINTS)]
            try:
                r = self.s.post(url, data={"data": q}, timeout=OVERPASS_TIMEOUT)
                self.last_call = time.time()
                self.calls += 1
                if r.status_code in (429, 504, 502, 503):
                    last_err = RuntimeError(f"overpass HTTP {r.status_code}")
                    time.sleep(self.slot_wait(url) if url.startswith("https://overpass-api.de") else 10 * (attempt + 1))
                    continue
                if r.status_code >= 400:
                    raise RuntimeError(f"overpass HTTP {r.status_code}: {r.text[:200]}")
                data = r.json()
                return data.get("elements", [])
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"overpass failed after retries: {last_err}")


def normalize_element(el: dict[str, Any], *, category: str, state: str, county: str) -> dict[str, Any] | None:
    tags = el.get("tags") or {}
    name = norm_space(tags.get("name") or tags.get("brand") or tags.get("official_name"))
    if not name:
        return None
    osm_type = str(el.get("type") or "")
    osm_id = str(el.get("id") or "")
    if osm_type not in ("node", "way", "relation") or not osm_id:
        return None
    house = norm_space(tags.get("addr:housenumber"))
    street = norm_space(tags.get("addr:street"))
    address_line1 = norm_space(" ".join(p for p in (house, street) if p)) or None
    city = norm_space(tags.get("addr:city")) or None
    postal = norm_space(tags.get("addr:postcode")) or None
    website = norm_space(tags.get("website") or tags.get("contact:website") or tags.get("url")) or None
    if website and not re.match(r"^https?://", website, re.I):
        website = "https://" + website
    phone = norm_space(tags.get("phone") or tags.get("contact:phone")) or None
    email = norm_space(tags.get("email") or tags.get("contact:email")) or None
    if email and not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
        email = None
    lat = el.get("lat") or (el.get("center") or {}).get("lat")
    lon = el.get("lon") or (el.get("center") or {}).get("lon")
    source_url = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
    return {
        "business_name": name,
        "normalized_business_name": norm_key(name),
        "phone": phone,
        "website_url": website,
        "email": email,
        "address_line1": address_line1,
        "city": city,
        "postal_code": postal,
        "source_url": source_url,
        "notes": PRIVATE_REVIEW_NOTE,
        "private_review_required": True,
        "publication_status": "private_review_required",
        "source_identifier": f"osm:{osm_type}:{osm_id}",
        "category_slug": category,
        "source_name": SOURCE_NAME,
        "source_lane_code": SOURCE_FAMILY,
        "state": state,
        "county": county,
        "latitude": lat,
        "longitude": lon,
        "opening_hours": tags.get("opening_hours"),
        "cuisine": tags.get("cuisine"),
        "osm_tags": {k: v for k, v in tags.items() if k in (
            "amenity", "shop", "craft", "office", "leisure", "healthcare", "cuisine", "takeaway",
            "delivery", "brand", "operator", "wheelchair", "opening_hours")},
        "dedupe_keys": {
            "source_identifier": f"osm:{osm_type}:{osm_id}",
            "normalized_business_name": norm_key(name),
            "phone": phone,
            "normalized_address": norm_key(f"{address_line1 or ''} {city or ''} {state} {postal or ''}"),
            "website_url": website,
            "source_url": source_url,
        },
    }


def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        k = r["source_identifier"]
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


# --- worker -------------------------------------------------------------------------------------

class Worker:
    def __init__(self, lane_code: str, *, hours: float, max_cells: int | None, dry_run: bool) -> None:
        self.lane_code = lane_code.upper()
        self.hours = hours
        self.max_cells = max_cells
        self.dry_run = dry_run
        self.supa = Supa()
        self.overpass = Overpass()
        host = re.sub(r"[^A-Za-z0-9]", "", socket.gethostname())[:12] or "pc"
        self.execution_id = f"{self.lane_code}-{host}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        self.worker_name = f"ypoai-pc-{self.lane_code}"
        self.lane: dict[str, Any] = {}
        self.stats = {"cells": 0, "inserted": 0, "duplicates": 0, "blocked": 0, "errors": 0, "overpass_calls": 0}

    # lane + control
    def load_lane(self) -> None:
        rows = self.supa.rpc("nx_read_lane", {"p_lane_code": self.lane_code})
        if not rows:
            raise SystemExit(f"Lane {self.lane_code} not found in nx_worker_lanes.")
        self.lane = rows[0]
        if not self.lane.get("enabled"):
            raise SystemExit(f"Lane {self.lane_code} is disabled.")
        if self.lane.get("lane_kind") != "collect":
            raise SystemExit(f"Lane {self.lane_code} is a {self.lane.get('lane_kind')} lane; this worker only collects.")
        control = self.supa.rpc("nexa_read_collector_control", {}) or {}
        if control and control.get("collection_enabled") is False:
            raise SystemExit("collection_enabled=false in nexa_collector_control; refusing to run.")

    def heartbeat(self, status: str, target_id: str | None = None, **details: Any) -> None:
        if self.dry_run:
            return
        try:
            self.supa.rpc("nexa_write_heartbeat", {
                "p_component_name": COMPONENT_NAME, "p_execution_id": self.execution_id, "p_status": status,
                "p_target_id": target_id, "p_details": {"lane": self.lane_code, "source_family": SOURCE_FAMILY, **details},
            })
        except Exception as exc:  # noqa: BLE001
            log("heartbeat_failed", error=str(exc)[:200])

    # claiming
    def claim(self) -> list[dict[str, Any]]:
        states = list(self.lane.get("priority_order") or self.lane.get("states") or [])
        batch = int(self.lane.get("batch_limit") or 25)
        per_state = max(1, min(30, batch // max(1, len(states))))
        rows = self.supa.rpc("agenarys_durable_next_targets", {"p_states": states, "p_limit_per_state": per_state}) or []
        # keep lane priority order (first states first), then the RPC's own order
        rank = {s: i for i, s in enumerate(states)}
        rows.sort(key=lambda r: (rank.get(r.get("state"), 99), r.get("county") or "", r.get("category_slug") or ""))
        return rows

    def peek_ready(self, limit: int) -> list[dict[str, Any]]:
        states = list(self.lane.get("priority_order") or self.lane.get("states") or [])
        return self.supa.rpc("nx_peek_ready", {"p_states": states, "p_limit": limit}) or []

    # closing
    def close(self, target: dict[str, Any], outcome: str, reason: str | None = None, inserted: int = 0) -> None:
        if self.dry_run:
            return
        self.supa.rpc("nx_close_cell", {"p_target_id": target["id"], "p_outcome": outcome, "p_reason": reason,
                                        "p_inserted": inserted, "p_execution_id": self.execution_id})

    def block(self, target: dict[str, Any], reason: str) -> None:
        self.stats["blocked"] += 1
        self.close(target, "blocked", reason)

    def complete(self, target: dict[str, Any], inserted: int) -> None:
        self.close(target, "completed", "overpass_pass_complete", inserted)

    # one cell
    def process(self, target: dict[str, Any]) -> None:
        state = str(target.get("state") or "").upper()
        county = norm_space(target.get("county"))
        category = str(target.get("category_slug") or "").lower()
        tid = target.get("id")
        self.stats["cells"] += 1
        selectors = CATEGORY_SELECTORS.get(category)
        if not selectors:
            log("cell_blocked", state=state, county=county, category=category, reason="no_free_deterministic_source_for_category")
            self.block(target, "no_free_deterministic_source_for_category")
            return
        if state != "DC" and not county_has_suffix(county):
            log("cell_blocked", state=state, county=county, category=category, reason="unsuffixed_county_name_duplicate_key")
            self.block(target, "unsuffixed_county_name_duplicate_key")
            return
        self.heartbeat("progress", tid, state=state, county=county, category=category, phase="overpass")
        query = build_overpass_query(state, county, selectors)
        try:
            elements = self.overpass.query(query)
        except Exception as exc:  # noqa: BLE001
            self.stats["errors"] += 1
            log("cell_error", state=state, county=county, category=category, error=str(exc)[:200])
            self.heartbeat("error", tid, state=state, county=county, category=category, error_fingerprint="overpass-error", message=str(exc)[:200])
            self.close(target, "ready", "overpass_error_released")
            return
        self.stats["overpass_calls"] = self.overpass.calls
        rows = dedupe([r for r in (normalize_element(e, category=category, state=state, county=county) for e in elements) if r])
        rows = rows[:MAX_ROWS_PER_CELL]
        if not rows:
            log("cell_empty", state=state, county=county, category=category, elements=len(elements))
            self.block(target, "osm_no_named_results")
            self.heartbeat("completed", tid, state=state, county=county, category=category, rows_collected=0, outcome="empty")
            return
        if self.dry_run:
            log("cell_dry_run", state=state, county=county, category=category, rows=len(rows),
                sample=[{"name": r["business_name"], "city": r["city"], "phone": r["phone"], "website": r["website_url"]} for r in rows[:3]])
            return
        result = self.supa.rpc("agenarys_durable_record_collection", {
            "p_worker_name": self.worker_name, "p_state": state, "p_county": county, "p_category_slug": category,
            "p_source_name": SOURCE_NAME, "p_source_url": OVERPASS_ENDPOINTS[0],
            "p_rows": rows,
            "p_note": "Overpass county bulk batch. Private review required before publication.",
            "p_payload": {"adapter": "ypoai_pc_worker", "lane": self.lane_code, "execution_id": self.execution_id,
                          "selectors": selectors, "elements": len(elements), "private_review_required": True},
        })
        res = (result[0] if isinstance(result, list) and result else result) or {}
        inserted = int(res.get("inserted_count") or 0)
        dupes = int(res.get("duplicate_count") or 0)
        self.stats["inserted"] += inserted
        self.stats["duplicates"] += dupes
        self.complete(target, inserted)
        log("cell_done", state=state, county=county, category=category, rows=len(rows), inserted=inserted, duplicates=dupes)
        self.heartbeat("completed", tid, state=state, county=county, category=category, rows_collected=inserted, duplicates=dupes)

    # main loop
    def run(self) -> int:
        self.load_lane()
        log("worker_start", lane=self.lane_code, execution_id=self.execution_id, states=self.lane.get("priority_order"),
            hours=self.hours, dry_run=self.dry_run, max_cells=self.max_cells)
        self.heartbeat("started", None, hours=self.hours)
        deadline = time.time() + self.hours * 3600
        idle_rounds = 0
        while time.time() < deadline:
            if self.max_cells is not None and self.stats["cells"] >= self.max_cells:
                break
            targets = self.peek_ready(self.max_cells or 3) if self.dry_run else self.claim()
            if not targets:
                idle_rounds += 1
                log("no_targets", idle_rounds=idle_rounds)
                if self.dry_run or idle_rounds >= 3:
                    break
                time.sleep(60)
                continue
            idle_rounds = 0
            for t in targets:
                if time.time() >= deadline or (self.max_cells is not None and self.stats["cells"] >= self.max_cells):
                    self.close(t, "ready", "worker_deadline_released")
                    continue
                try:
                    self.process(t)
                except Exception as exc:  # noqa: BLE001
                    self.stats["errors"] += 1
                    log("cell_error", target=t.get("id"), error=str(exc)[:200])
                    self.heartbeat("error", t.get("id"), error_fingerprint="record-error", message=str(exc)[:200])
            if self.dry_run:
                break
        log("worker_stop", **self.stats)
        self.heartbeat("completed", None, rows_collected=self.stats["inserted"], **{k: v for k, v in self.stats.items() if k != "inserted"})
        return 0


def cmd_status(lane_code: str) -> int:
    supa = Supa()
    rows = supa.rpc("nx_lane_status", {"p_lane_code": lane_code.upper()}) or []
    if not rows:
        print(f"lane {lane_code}: not found")
        return 1
    r = rows[0]
    print(f"lane {r['lane_code']}  enabled={r['enabled']}  states={r['state_count']}")
    print(f"  ready cells      : {r['ready_cells']}")
    print(f"  collecting now   : {r['collecting_now']}")
    print(f"  completed cells  : {r['completed_cells']}")
    print(f"  rows last 24h    : {r['rows_last_24h']}")
    print(f"  last heartbeat   : {r['last_heartbeat']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("status"); s.add_argument("--lane", required=True)
    r = sub.add_parser("start"); r.add_argument("--lane", required=True); r.add_argument("--hours", type=float, default=8.0); r.add_argument("--max-cells", type=int)
    d = sub.add_parser("dry-run"); d.add_argument("--lane", required=True); d.add_argument("--max-cells", type=int, default=3)
    a = p.parse_args(argv)
    if a.cmd == "status":
        return cmd_status(a.lane)
    if a.cmd == "start":
        return Worker(a.lane, hours=a.hours, max_cells=a.max_cells, dry_run=False).run()
    return Worker(a.lane, hours=1.0, max_cells=a.max_cells, dry_run=True).run()


if __name__ == "__main__":
    sys.exit(main())
