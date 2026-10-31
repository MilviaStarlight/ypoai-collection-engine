#!/usr/bin/env python3
"""
YPOAI Overture Maps places collector: one run per state, a second free source next to OpenStreetMap.

OpenStreetMap is thin in many places (Puerto Rico above all: Dorado has 5 bakeries and no barbers in
OSM). Overture Maps publishes an open places dataset (CDLA-Permissive-2.0 / Apache-2.0 / CC0 sources:
Meta, Microsoft, Foursquare, AllThePlaces and others) as Parquet files on a public S3 bucket. This
script reads only the row groups inside the state's bounding box, maps each place's Overture category
to a YPOAI category, assigns its county with the same Census boundaries the OSM extractor uses, and
records the rows through the same durable dedupe path (nx_record_bulk). Rows that match a business
already collected from OSM are counted as duplicates by that path.

No model calls. No per-request calls to public map servers. Nothing is written to disk except the
Census county file, which is deleted.

Usage:
  python scripts/overture_extract.py --state PR --dry-run       # count only, writes nothing
  python scripts/overture_extract.py --state PR                 # full run
  python scripts/overture_extract.py --state PR --county "Dorado Municipio" --dry-run

Environment: SUPABASE_SERVICE_ROLE_KEY (required unless --dry-run), SUPABASE_URL (optional),
OVERTURE_RELEASE (optional, e.g. 2026-10-21.0; default = newest release in the bucket).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(__file__))
import state_extract as se  # noqa: E402  (states, counties, logging, download)
import worker  # noqa: E402  (normalizers, Supabase client)

BUCKET = "overturemaps-us-west-2"
REGION = "us-west-2"
SOURCE_NAME = "Overture Maps places"
SOURCE_FAMILY = "overture_places"
COMPONENT = "gh-worker"
CHUNK_ROWS = 500
MIN_CONFIDENCE = 0.5  # Overture's own guidance: below this many places are stale or misplaced
CLOSED = {"permanently_closed", "temporarily_closed", "closed"}

# Overture category (taxonomy primary or any level of its hierarchy) -> YPOAI category slug.
# The most specific level is checked first, so "barber" wins over "personal_or_beauty_service".
OVERTURE_TO_SLUG: dict[str, str] = {
    # food
    "restaurant": "restaurant", "bar_and_grill_restaurant": "restaurant", "pizza_restaurant": "restaurant",
    "bakery": "cafes-bakeries", "coffee_shop": "cafes-bakeries", "cafe": "cafes-bakeries", "dessert_shop": "cafes-bakeries",
    "food_truck_stand": "food-trucks", "smoothie_juice_bar": "juice-bars", "juice_bar": "juice-bars",
    "caterer": "catering", "catering_service": "catering",
    # beauty and personal care
    "barber": "barber", "beauty_salon": "hair-salon", "hair_salon": "hair-salon", "nail_salon": "nail-salon",
    "eyelash_service": "lash-studios", "medical_spa": "med-spa", "spa": "spa", "day_spa": "spa",
    "massage_therapy": "massage-studios", "massage": "massage-studios", "tattoo_and_piercing": "tattoo-shops",
    # health
    "dental_clinic": "dentist", "dentist": "dentist", "chiropractic": "chiropractor", "chiropractor": "chiropractor",
    "physical_therapy": "physical-therapy", "urgent_care_clinic": "urgent-care", "doctors_office": "medical-services",
    "family_practice": "medical-services", "primary_care_or_general_clinic": "medical-services",
    "veterinarian": "veterinarian", "pet_groomer": "pet-groomers", "pet_boarding": "pet-boarding",
    "dog_trainer": "dog-trainers", "dog_walker": "dog-walkers", "retirement_home": "elder-care",
    "assisted_living_facility": "elder-care", "day_care_preschool": "child-care", "child_care_and_day_care": "child-care",
    "tutoring_center": "tutoring",
    # auto
    "auto_body_shop": "auto-body", "automotive_repair": "auto-repair", "auto_dealer": "car-dealerships",
    "car_dealer": "car-dealerships", "auto_detailing": "car-detailing", "car_wash": "car-wash",
    "tire_dealer_and_repair": "tire-shops", "towing_service": "towing", "roadside_assistance": "roadside-assistance",
    "car_window_tinting": "glass-repair", "auto_glass_service": "glass-repair",
    # shops
    "clothing_store": "clothing-stores", "womens_clothing_store": "clothing-stores", "mens_clothing_store": "clothing-stores",
    "childrens_clothing_store": "clothing-stores", "fashion_boutique": "clothing-stores",
    "furniture_store": "furniture-stores", "jewelry_store": "jewelry-stores", "home_goods_store": "home-decor",
    "flowers_and_gifts_store": "specialty-shops", "gift_shop": "specialty-shops", "flooring_store": "flooring",
    "mobile_phone_repair": "phone-repair", "it_service_and_computer_repair": "computer-repair",
    "home_security": "security-systems",
    # professional services
    "accountant": "accounting", "tax_preparation": "tax-preparation", "tax_services": "tax-preparation",
    "insurance_agency": "insurance-agency", "attorney_or_law_firm": "legal-services", "legal_service": "legal-services",
    "real_estate_agent": "real-estate-agent", "property_management": "property-management",
    "business_consulting": "business-consulting", "life_coach": "life-business-coaches",
    "party_and_event_planning": "event-planning", "event_planning": "event-planning",
    "event_photography_service": "photography", "photographer": "photography",
    "funeral_service": "funeral-home", "movers": "moving", "moving_company": "moving",
    # fitness
    "gym": "gyms", "fitness_trainer": "personal-trainer", "personal_trainer": "personal-trainer",
    "yoga_studio": "yoga-studios", "pilates_studio": "pilates", "boxing_gym": "boxing-studios",
    # home services
    "plumber": "plumbing", "plumbing": "plumbing", "electrician": "electrical", "hvac_service": "hvac",
    "roofing": "roofing", "painting": "painting", "landscaping": "landscaping", "lawn_service": "lawn-care",
    "tree_service": "tree-service", "pest_control_service": "pest-control", "pest_control": "pest-control",
    "home_cleaning": "cleaning-services", "janitorial_service": "cleaning", "carpet_cleaning": "carpet-cleaning",
    "pressure_washing": "pressure-washing", "pool_cleaning": "pool-service", "swimming_pool_service": "pool-service",
    "locksmith": "locksmith", "handyman": "handyman", "contractor": "remodeling", "remodeling": "remodeling",
    "windows_installation": "window-installation", "garage_door_service": "garage-door", "fence_and_gate_service": "fencing",
    "solar_installation": "solar", "junk_removal_and_hauling": "junk-removal", "septic_services": "septic",
    "gutter_service": "gutter", "welder": "welding", "cabinetry": "cabinetry", "masonry_concrete": "masonry",
    "home_inspector": "home-inspection", "water_damage_restoration": "water-damage-restoration",
    "mold_remediation": "mold-remediation",
}


def slug_for(taxonomy: dict | None, basic: str | None) -> str | None:
    tx = taxonomy or {}
    levels = list(reversed(tx.get("hierarchy") or [])) or [tx.get("primary")]
    if tx.get("primary") and tx["primary"] not in levels:
        levels.insert(0, tx["primary"])
    levels.append(basic)
    for lv in levels:
        if lv and lv in OVERTURE_TO_SLUG:
            slug = OVERTURE_TO_SLUG[lv]
            return slug if slug in worker.CATEGORY_SELECTORS else None
    return None


def latest_release() -> str:
    env = os.environ.get("OVERTURE_RELEASE")
    if env:
        return env
    r = requests.get(f"https://{BUCKET}.s3.amazonaws.com/", params={"list-type": "2", "prefix": "release/", "delimiter": "/"},
                     timeout=60)
    r.raise_for_status()
    rel = sorted(re.findall(r"<Prefix>release/([^/<]+)/</Prefix>", r.text))
    if not rel:
        raise SystemExit("no Overture release found")
    return rel[-1]


def read_places(release: str, bounds: tuple[float, float, float, float]):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    s3 = pafs.S3FileSystem(anonymous=True, region=REGION, **({"proxy_options": proxy} if proxy else {}))
    d = ds.dataset(f"{BUCKET}/release/{release}/theme=places/type=place/", filesystem=s3, format="parquet")
    minx, miny, maxx, maxy = bounds
    x, y = pc.field("bbox", "xmin"), pc.field("bbox", "ymin")
    flt = (x >= minx) & (x <= maxx) & (y >= miny) & (y <= maxy)
    cols = ["id", "names", "basic_category", "taxonomy", "phones", "websites", "emails", "addresses", "bbox",
            "confidence", "operating_status"]
    for batch in d.to_batches(columns=cols, filter=flt):
        yield from batch.to_pylist()


def to_row(p: dict[str, Any], *, category: str, state: str, county: str, release: str) -> dict[str, Any] | None:
    name = worker.norm_space(((p.get("names") or {}).get("primary")))
    if not name or not p.get("id"):
        return None
    addr = (p.get("addresses") or [{}])[0] or {}
    address_line1 = worker.norm_space(addr.get("freeform")) or None
    city = worker.norm_space(addr.get("locality")) or None
    postal = worker.norm_space(addr.get("postcode")) or None
    website = worker.norm_space((p.get("websites") or [None])[0]) or None
    if website and not re.match(r"^https?://", website, re.I):
        website = "https://" + website
    phone = worker.norm_space((p.get("phones") or [None])[0]) or None
    email = worker.norm_space((p.get("emails") or [None])[0]) or None
    if email and not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
        email = None
    bb = p.get("bbox") or {}
    lat = (bb.get("ymin") + bb.get("ymax")) / 2 if bb.get("ymin") is not None else None
    lon = (bb.get("xmin") + bb.get("xmax")) / 2 if bb.get("xmin") is not None else None
    sid = f"overture:{p['id']}"
    source_url = f"https://explore.overturemaps.org/#16/{lat}/{lon}" if lat is not None else f"https://overturemaps.org/?{p['id']}"
    key = worker.norm_key(name)
    return {
        "business_name": name,
        "normalized_business_name": key,
        "phone": phone,
        "website_url": website,
        "email": email,
        "address_line1": address_line1,
        "city": city,
        "postal_code": postal,
        "source_url": source_url,
        "notes": worker.PRIVATE_REVIEW_NOTE,
        "private_review_required": True,
        "publication_status": "private_review_required",
        "source_identifier": sid,
        "category_slug": category,
        "source_name": SOURCE_NAME,
        "source_lane_code": SOURCE_FAMILY,
        "state": state,
        "county": county,
        "latitude": lat,
        "longitude": lon,
        "overture": {"release": release, "category": ((p.get("taxonomy") or {}).get("primary")),
                     "confidence": p.get("confidence")},
        "dedupe_keys": {
            "source_identifier": sid,
            "normalized_business_name": key,
            "phone": phone,
            "normalized_address": worker.norm_key(f"{address_line1 or ''} {city or ''} {state} {postal or ''}"),
            "website_url": website,
            "source_url": source_url,
        },
    }


def run(state: str, counties_zip: str | None, only_county: str | None, dry_run: bool) -> int:
    state = state.upper()
    if state not in se.STATES:
        raise SystemExit(f"unknown state {state}")
    fips, _slug = se.STATES[state]
    execution_id = f"O-{state}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    supa = None if dry_run else worker.Supa()

    def hb(status: str, **details: Any) -> None:
        if supa is None:
            return
        try:
            supa.rpc("nexa_write_heartbeat", {"p_component_name": COMPONENT, "p_execution_id": execution_id, "p_status": status,
                                              "p_target_id": None, "p_details": {"state": state, "source_family": SOURCE_FAMILY, **details}})
        except Exception as exc:  # noqa: BLE001
            se.log("heartbeat_failed", error=str(exc)[:200])

    work = tempfile.mkdtemp(prefix=f"ypoai-ov-{state}-")
    t0 = time.time()
    try:
        hb("started")
        release = latest_release()
        if not counties_zip:
            counties_zip = os.path.join(work, "counties.zip")
            se.download(se.COUNTIES_URL, counties_zip)
        tree, geoms, names = se.load_counties(counties_zip, fips)
        if only_county:
            keep = [i for i, n in enumerate(names) if n.lower() == only_county.lower()]
            if not keep:
                raise SystemExit(f"county not found: {only_county}")
            from shapely.strtree import STRtree
            geoms = [geoms[i] for i in keep]; names = [names[i] for i in keep]; tree = STRtree(geoms)
        xs = [g.bounds for g in geoms]
        bounds = (min(b[0] for b in xs), min(b[1] for b in xs), max(b[2] for b in xs), max(b[3] for b in xs))
        se.log("overture_read", state=state, release=release, bounds=[round(v, 3) for v in bounds])

        rows: list[dict[str, Any]] = []
        seen = skipped_low = skipped_closed = unmapped = outside = 0
        for p in read_places(release, bounds):
            seen += 1
            if (p.get("confidence") or 0) < MIN_CONFIDENCE:
                skipped_low += 1; continue
            if (p.get("operating_status") or "") in CLOSED:
                skipped_closed += 1; continue
            cat = slug_for(p.get("taxonomy"), p.get("basic_category"))
            if not cat:
                unmapped += 1; continue
            bb = p.get("bbox") or {}
            if bb.get("xmin") is None:
                continue
            county = se.county_of(tree, geoms, names, (bb["xmin"] + bb["xmax"]) / 2, (bb["ymin"] + bb["ymax"]) / 2)
            if not county:
                outside += 1; continue
            row = to_row(p, category=cat, state=state, county=county, release=release)
            if row:
                rows.append(row)
        groups: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
            groups[(r["county"], r["category_slug"])] += 1
        se.log("matched", state=state, places=seen, rows=len(rows), low_confidence=skipped_low, closed=skipped_closed,
               unmapped=unmapped, outside_state=outside, groups=len(groups), seconds=round(time.time() - t0, 1))

        if dry_run:
            by_cat = Counter(r["category_slug"] for r in rows)
            se.log("dry_run_categories", top=by_cat.most_common(25),
                   with_phone=sum(1 for r in rows if r["phone"]), with_website=sum(1 for r in rows if r["website_url"]))
            return 0

        total_ins = total_dup = 0
        rows.sort(key=lambda r: (r["county"], r["category_slug"]))
        for i in range(0, len(rows), CHUNK_ROWS):
            chunk = rows[i:i + CHUNK_ROWS]
            for attempt in range(5):
                try:
                    res = supa.rpc("nx_record_bulk", {"p_worker_name": f"ypoai-overture-{state}", "p_state": state,
                                                      "p_source_name": SOURCE_NAME,
                                                      "p_source_url": f"s3://{BUCKET}/release/{release}/theme=places",
                                                      "p_rows": chunk, "p_payload": {"adapter": "ypoai_overture_extract",
                                                                                     "execution_id": execution_id,
                                                                                     "private_review_required": True}}) or []
                    break
                except Exception as exc:  # noqa: BLE001
                    se.log("record_retry", attempt=attempt + 1, error=str(exc)[:200])
                    time.sleep(30 * 2 ** attempt)
            else:
                hb("error", error_fingerprint="record-failed")
                raise SystemExit("recording failed five times; stopping this state")
            for g in res:
                total_ins += int(g.get("inserted_count") or 0)
                total_dup += int(g.get("duplicate_count") or 0)
            hb("progress", rows_collected=0, recorded=min(i + CHUNK_ROWS, len(rows)), of=len(rows))
        se.log("state_done", state=state, inserted=total_ins, duplicates=total_dup, seconds=round(time.time() - t0, 1))
        hb("completed", rows_collected=total_ins, duplicates=total_dup)
        return 0
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        se.log("state_error", state=state, error=str(exc)[:300])
        hb("error", error_fingerprint="overture-extract-error", message=str(exc)[:200])
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True)
    ap.add_argument("--counties")
    ap.add_argument("--county", help="limit to one county by its Census name, e.g. 'Dorado Municipio'")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    return run(a.state, a.counties, a.county, a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
