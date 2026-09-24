#!/usr/bin/env python3
"""
Team B continuous collector for Agenarys.

This runner keeps the non-OSM lane moving, reports live metrics, deprioritizes
Arkansas until other Team B states catch up, and re-awakens the GitHub Actions
swarm when pending work still exists.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


PROJECT_ID = "ehzxnnxyliapvcvzzmyp"
SUPABASE_SCHEMA = "business_intelligence"
SUPABASE_PUBLIC_SCHEMA = "public"
SOURCE_NAME_NPI = "NPPES NPI Registry API"
SOURCE_NAME_OSM_FALLBACK = "OpenStreetMap Nominatim fallback"
COORDINATOR_NAME = "Team B Coordinator Boss"
PRIVATE_REVIEW_NOTE = "Private review required before publication."
TEAM_B_DISPATCH_EVENT = "awaken_swarm"
DISPATCH_COOLDOWN_SECONDS = 15 * 60
DEFAULT_BURST_LIMIT = int(os.environ.get("TEAM_B_MAX_BATCHES_PER_RUN", "1"))
DEFAULT_PAGE_SIZE = 5000
DEFAULT_CONTINUOUS_SLEEP_SECONDS = int(os.environ.get("TEAM_B_CONTINUOUS_SLEEP_SECONDS", "15"))
SUPABASE_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("TEAM_B_SUPABASE_TIMEOUT_SECONDS", "180"))
TARGET_PAIR_HISTORY_FILE = Path(
    os.environ.get("TEAM_B_TARGET_PAIR_HISTORY_FILE", "logs/team_target_pair_history.jsonl")
)
NON_OSM_ATTEMPT_HISTORY_FILE = Path(
    os.environ.get("TEAM_B_NON_OSM_ATTEMPT_HISTORY_FILE", "logs/team_non_osm_attempts.jsonl")
)
LOG_FILE_PATH = os.environ.get("LOG_FILE_PATH", "").strip()
TARGET_PAIR_REVISIT_WINDOW = timedelta(hours=48)
# Short emergency escape hatch used only when the 48-hour policy has fully starved the queue.
TARGET_PAIR_REPAIR_WINDOW = timedelta(hours=21)
NON_OSM_SOURCE_STALE_WINDOW = timedelta(minutes=15)
AR_STATE = "AR"
TEAM_B_NEXT_TARGET_RPC = "agenarys_durable_next_targets"
PHOTON_SEARCH_URL = "https://photon.komoot.io/api/"
OSM_NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OSM_NOMINATIM_SOURCE_LANE = "osm_fallback"
OSM_PHOTON_SOURCE_LANE = "osm_photon"
OSM_FALLBACK_LIMIT = 10
OSM_FALLBACK_NICHES = {
    "event-planners",
    "wedding-planners",
    "party-planners",
    "event-coordinators",
    "djs",
    "entertainment-services",
    "party-rental-companies",
    "event-equipment-rental",
    "photo-booth-rental",
    "wedding-venues",
    "event-venues",
    "retail-stores",
    "clothing-stores",
    "shoe-stores",
    "jewelry-stores",
    "furniture-stores",
    "home-decor-stores",
    "convenience-stores",
    "specialty-retail",
    "e-commerce-sellers",
    "restaurants",
    "cafe",
    "cafes",
    "coffee-shops",
    "food-trucks",
    "bakeries",
    "catering-companies",
    "juice-bars",
    "ice-cream-shops",
    "photographers",
    "videographers",
    "video-editors",
    "graphic-designers",
    "content-creators",
    "podcast-producers",
}
US_51_JURISDICTIONS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
]
class AdapterError(RuntimeError):
    pass


TEAM_B_CATEGORY_SLUGS = [
    "doctor",
    "dentist",
    "medical-services",
    "urgent-care",
    "physical-therapy",
    "chiropractors",
    "pharmacies",
    "optometrists",
    "veterinarians",
    "medical-clinics",
    "mental-health-clinics",
    "contractor",
    "contractors",
    "plumbing",
    "electrical",
    "hvac",
    "roofing",
    "restaurant",
    "restaurants",
    "food",
    "food-establishment",
    "food-establishments",
    "catering",
    "coffee-shop",
    "coffee-shops",
    "cafe",
    "cafes",
    "childcare",
    "child-care",
    "real-estate-agent",
    "real-estate",
    "insurance-agency",
    "insurance",
    "cosmetology",
    "barber",
    "hair-salon",
    "nail-salon",
]

TEAM_B_HEALTHCARE_CATEGORIES = {
    "doctor",
    "dentist",
    "medical-services",
    "urgent-care",
    "physical-therapy",
    "chiropractors",
    "pharmacies",
    "optometrists",
    "veterinarians",
    "medical-clinics",
    "mental-health-clinics",
}

NICHES_ENV_DEFAULT = ""

TARGET_PAIR_LAST_SEEN: dict[tuple[str, str], datetime] = {}
LAST_NON_OSM_ATTEMPT_AT: datetime | None = None


@dataclass(frozen=True)
class CollectorConfig:
    supabase_url: str
    service_role_key: str
    github_token: str
    github_repository: str
    worker_slot: int
    boss_check: bool
    burst_limit: int


def parse_worker_slot(raw: str | None) -> int:
    if not raw:
        return 1
    try:
        slot = int(raw)
    except ValueError as exc:
        raise SystemExit(f"Invalid WORKER_SLOT value: {raw!r}") from exc
    if slot < 1:
        raise SystemExit("WORKER_SLOT must be >= 1")
    return slot


def stable_bucket(text: str, buckets: int = 8) -> int:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % buckets


def normalize_state(state: str | None) -> str:
    return (state or "").strip().upper()


def normalize_category(category: str | None) -> str:
    return (category or "").strip().lower().replace("_", "-")


def normalize_spaces(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name_key(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", "", normalize_spaces(value).upper())


def normalize_address_key(*parts: str | None) -> str:
    joined = " ".join(normalize_spaces(part).upper() for part in parts if normalize_spaces(part))
    return re.sub(r"[^A-Z0-9]+", "", joined)


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        keys = row.get("dedupe_keys") or {}
        preferred_key = ("source_identifier", str(keys.get("source_identifier") or ""))
        fallback_key = (
            "business",
            str(keys.get("normalized_business_name") or ""),
            str(keys.get("phone") or ""),
            str(keys.get("normalized_address") or ""),
            str(keys.get("website_url") or ""),
        )
        key = preferred_key if preferred_key[1] else fallback_key
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def parse_collect_niches() -> list[str]:
    raw = os.environ.get("COLLECTOR_NICHES", NICHES_ENV_DEFAULT).strip()
    if not raw:
        return []
    return [normalize_category(part) for part in raw.split(",") if normalize_category(part)]


def niche_tokens_for_category(category_slug: str) -> set[str]:
    category = normalize_category(category_slug)
    tokens = {category}
    if category.startswith("doctor") or category in TEAM_B_HEALTHCARE_CATEGORIES:
        tokens.update({"doctor", "medical-services", "clinic", "clinics"})
    if any(token in category for token in ("restaurant", "food", "cafe", "coffee", "catering", "bakery", "juice", "ice-cream")):
        tokens.update({"restaurant", "restaurants", "cafe", "cafes", "coffee", "coffee-shops", "food", "food-establishment", "food-establishments", "catering", "bakery", "juice-bars", "ice-cream-shops"})
    if any(token in category for token in ("contract", "plumb", "electrical", "hvac", "roof", "remodel", "handyman", "repair")):
        tokens.update({"contractor", "contractors", "plumbing", "electrical", "hvac", "roofing", "auto-repair-shops", "car-detailing", "towing-services"})
    if any(token in category for token in ("cosmetology", "barber", "hair", "nail", "esthetic", "massage", "tattoo", "salon", "spa")):
        tokens.update({"cosmetology", "barber", "hair-salon", "nail-salon", "estheticians", "massage-therapists", "tattoo-shops", "day-spas"})
    if "child" in category:
        tokens.update({"childcare", "child-care"})
    if any(token in category for token in ("real-estate", "insurance")):
        tokens.update({"real-estate", "real-estate-agent", "insurance", "insurance-agency"})
    if any(token in category for token in ("pet", "dog")):
        tokens.update({"pet-groomers", "dog-trainers", "pet-boarding-facilities", "dog-daycare", "pet-sitters"})
    if any(token in category for token in ("event", "wedding", "party", "djs", "photographer", "video", "content", "graphic")):
        tokens.update({"event-planners", "wedding-planners", "djs", "event-venues", "wedding-venues", "photographers", "videographers", "video-editors", "graphic-designers", "content-creators"})
    if any(token in category for token in ("retail", "store", "clothing", "jewelry", "furniture", "decor", "convenience", "e-commerce", "specialty-retail")):
        tokens.update({"retail-stores", "clothing-stores", "shoe-stores", "jewelry-stores", "furniture-stores", "home-decor-stores", "convenience-stores", "specialty-retail", "e-commerce-sellers"})
    return tokens


def uses_npi_adapter(category_slug: str) -> bool:
    return normalize_category(category_slug) in TEAM_B_HEALTHCARE_CATEGORIES


def uses_osm_fallback(category_slug: str) -> bool:
    category = normalize_category(category_slug)
    if category in OSM_FALLBACK_NICHES:
        return True
    return any(
        token in category
        for token in (
            "event",
            "wedding",
            "party",
            "photographer",
            "videographer",
            "retail",
            "store",
            "restaurant",
            "cafe",
            "coffee",
            "food",
            "bakery",
            "catering",
            "juice",
            "ice-cream",
        )
    )


def niche_allowed(category_slug: str, allowed_niches: list[str]) -> bool:
    if not allowed_niches:
        return True
    category_tokens = niche_tokens_for_category(category_slug)
    return any(normalize_category(niche) in category_tokens or niche_tokens_for_category(niche) & category_tokens for niche in allowed_niches)


def source_family_for_category(category_slug: str) -> str:
    category = normalize_category(category_slug)
    if uses_npi_adapter(category):
        return "npi_registry"
    if any(token in category for token in ("restaurant", "food", "cafe", "coffee", "catering")):
        return "restaurant_food"
    if any(token in category for token in ("contract", "plumb", "electrical", "hvac", "roof", "remodel", "handyman")):
        return "contractor_license"
    if any(token in category for token in ("cosmetology", "barber", "hair", "nail")):
        return "cosmetology_board"
    if "child" in category:
        return "childcare_license"
    if any(token in category for token in ("real-estate", "insurance")):
        return "real_estate_insurance"
    return "official_public_source"


def target_signature(row: dict[str, Any]) -> str:
    raw_id = row.get("target_id") or row.get("id")
    if raw_id is not None and str(raw_id).strip():
        return str(raw_id).strip()
    state = normalize_state(row.get("state"))
    county = (row.get("county") or "").strip().lower()
    category = normalize_category(row.get("category_slug"))
    digest = hashlib.sha256(f"{state}:{county}:{category}".encode("utf-8")).hexdigest()
    return digest[:12]


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def load_target_pair_history() -> None:
    global TARGET_PAIR_LAST_SEEN
    TARGET_PAIR_LAST_SEEN = {}
    if not TARGET_PAIR_HISTORY_FILE.exists():
        return
    try:
        for line in TARGET_PAIR_HISTORY_FILE.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            state = normalize_state(entry.get("state"))
            category = normalize_category(entry.get("category"))
            seen_at = parse_iso_timestamp(str(entry.get("seen_at") or ""))
            if not state or not category or not seen_at:
                continue
            key = (state, category)
            previous = TARGET_PAIR_LAST_SEEN.get(key)
            if previous is None or seen_at > previous:
                TARGET_PAIR_LAST_SEEN[key] = seen_at
    except Exception:
        TARGET_PAIR_LAST_SEEN = {}


def remember_target_pair(state: str, category: str, seen_at: datetime | None = None) -> None:
    state = normalize_state(state)
    category = normalize_category(category)
    if not state or not category:
        return
    seen_at = seen_at or datetime.now(timezone.utc)
    key = (state, category)
    previous = TARGET_PAIR_LAST_SEEN.get(key)
    if previous is not None and seen_at <= previous:
        return
    TARGET_PAIR_LAST_SEEN[key] = seen_at
    append_jsonl(
        TARGET_PAIR_HISTORY_FILE,
        {
            "state": state,
            "category": category,
            "seen_at": seen_at.isoformat(),
        },
    )


def target_pair_is_recent(
    state: str,
    category: str,
    now: datetime | None = None,
    revisit_window: timedelta = TARGET_PAIR_REVISIT_WINDOW,
) -> bool:
    state = normalize_state(state)
    category = normalize_category(category)
    last_seen = TARGET_PAIR_LAST_SEEN.get((state, category))
    if last_seen is None:
        return False
    now = now or datetime.now(timezone.utc)
    return now - last_seen < revisit_window


def filter_target_pairs_by_revisit_window(
    targets: list[dict[str, Any]],
    now: datetime,
    revisit_window: timedelta,
) -> list[dict[str, Any]]:
    return [
        row
        for row in targets
        if not target_pair_is_recent(
            normalize_state(row.get("state")),
            normalize_category(row.get("category_slug")),
            now,
            revisit_window,
        )
    ]


def load_non_osm_attempt_state() -> None:
    global LAST_NON_OSM_ATTEMPT_AT
    LAST_NON_OSM_ATTEMPT_AT = None
    if not NON_OSM_ATTEMPT_HISTORY_FILE.exists():
        return
    try:
        latest: datetime | None = None
        for line in NON_OSM_ATTEMPT_HISTORY_FILE.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            family = normalize_category(entry.get("family"))
            if family == "osm_fallback":
                continue
            seen_at = parse_iso_timestamp(str(entry.get("seen_at") or ""))
            if seen_at and (latest is None or seen_at > latest):
                latest = seen_at
        LAST_NON_OSM_ATTEMPT_AT = latest
    except Exception:
        LAST_NON_OSM_ATTEMPT_AT = None


def remember_non_osm_attempt(state: str, category: str, family: str, seen_at: datetime | None = None) -> None:
    family = normalize_category(family)
    if family == "osm_fallback":
        return
    state = normalize_state(state)
    category = normalize_category(category)
    if not state or not category:
        return
    seen_at = seen_at or datetime.now(timezone.utc)
    global LAST_NON_OSM_ATTEMPT_AT
    if LAST_NON_OSM_ATTEMPT_AT is None or seen_at > LAST_NON_OSM_ATTEMPT_AT:
        LAST_NON_OSM_ATTEMPT_AT = seen_at
    append_jsonl(
        NON_OSM_ATTEMPT_HISTORY_FILE,
        {
            "state": state,
            "category": category,
            "family": family,
            "seen_at": seen_at.isoformat(),
        },
    )


def non_osm_attempt_recent(now: datetime | None = None) -> bool:
    if LAST_NON_OSM_ATTEMPT_AT is None:
        return False
    now = now or datetime.now(timezone.utc)
    return now - LAST_NON_OSM_ATTEMPT_AT <= NON_OSM_SOURCE_STALE_WINDOW


def newest_log_file() -> Path | None:
    log_dir = Path("logs")
    if not log_dir.exists():
        return None
    candidates = [path for path in log_dir.glob("*.log") if path.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0]


def resolve_live_log_path() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    configured = Path(LOG_FILE_PATH) if LOG_FILE_PATH else None
    chosen = configured if configured and configured.exists() else None
    rebound = False
    if chosen is None:
        chosen = newest_log_file()
        rebound = chosen is not None
    else:
        age = now - datetime.fromtimestamp(chosen.stat().st_mtime, tz=timezone.utc)
        if age > timedelta(hours=24):
            latest = newest_log_file()
            if latest is not None and latest != chosen:
                chosen = latest
                rebound = True
    if chosen is None:
        return {"path": None, "rebound": False, "stale": False, "age_hours": None}
    age_hours = round((now - datetime.fromtimestamp(chosen.stat().st_mtime, tz=timezone.utc)).total_seconds() / 3600, 2)
    return {
        "path": str(chosen),
        "rebound": rebound,
        "stale": age_hours > 24,
        "age_hours": age_hours,
    }


def is_synthetic_target(row: dict[str, Any]) -> bool:
    metadata = row.get("metadata")
    return bool(isinstance(metadata, dict) and metadata.get("fallback"))


def osm_lane_paused(now: datetime | None = None) -> bool:
    return not non_osm_attempt_recent(now)


def category_priority(category_slug: str) -> int:
    family = source_family_for_category(category_slug)
    order = {
        "npi_registry": 0,
        "restaurant_food": 1,
        "contractor_license": 2,
        "cosmetology_board": 3,
        "childcare_license": 4,
        "real_estate_insurance": 5,
        "official_public_source": 6,
        "osm_fallback": 7,
    }
    return order.get(family, 99)


def state_priority(state: str) -> int:
    return 1000 if normalize_state(state) == AR_STATE else 0


def supabase_request(
    config: CollectorConfig,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    body: Any | None = None,
    prefer: str | None = None,
    range_header: str | None = None,
) -> tuple[Any, dict[str, str]]:
    base = config.supabase_url.rstrip("/")
    url = f"{base}/rest/v1/{path.lstrip('/')}"
    request_headers = {
        "apikey": config.service_role_key,
        "Authorization": f"Bearer {config.service_role_key}",
        "Accept-Profile": SUPABASE_SCHEMA,
        "Content-Profile": SUPABASE_SCHEMA,
    }
    if prefer:
        request_headers["Prefer"] = prefer
    if range_header:
        request_headers["Range"] = range_header
        request_headers["Range-Unit"] = "items"
    request_kwargs: dict[str, Any] = {
        "method": method.upper(),
        "url": url,
        "headers": request_headers,
        "timeout": SUPABASE_REQUEST_TIMEOUT_SECONDS,
    }
    if params:
        request_kwargs["params"] = params
    if body is not None:
        request_headers["Content-Type"] = "application/json"
        request_kwargs["json"] = body
    response = requests.request(**request_kwargs)
    response.raise_for_status()
    raw = response.text.strip() or "null"
    parsed = json.loads(raw)
    response_headers = dict(response.headers)
    return parsed, response_headers


def supabase_rpc(
    config: CollectorConfig,
    function_name: str,
    body: dict[str, Any],
) -> Any:
    base = config.supabase_url.rstrip("/")
    url = f"{base}/rest/v1/rpc/{function_name}"
    response = requests.post(
        url,
        headers={
            "apikey": config.service_role_key,
            "Authorization": f"Bearer {config.service_role_key}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=SUPABASE_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    raw = response.text.strip() or "null"
    return json.loads(raw)


def request_json(url: str, timeout_seconds: int = 30, retries: int = 3) -> Any:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AgenarysCollector/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(1.5 * (attempt + 1))
    raise AdapterError(f"Request failed after {retries} attempts: {url} :: {last_error}")


def supabase_get(config: CollectorConfig, path: str, *, params: dict[str, Any] | None = None, range_header: str | None = None) -> tuple[Any, dict[str, str]]:
    return supabase_request(config, "GET", path, params=params, range_header=range_header)


def supabase_post(config: CollectorConfig, path: str, body: Any, *, prefer: str = "return=representation") -> tuple[Any, dict[str, str]]:
    return supabase_request(config, "POST", path, body=body, prefer=prefer)


def supabase_patch(config: CollectorConfig, path: str, body: Any, *, params: dict[str, Any] | None = None) -> tuple[Any, dict[str, str]]:
    return supabase_request(config, "PATCH", path, params=params, body=body, prefer="return=representation")


def get_exact_count(config: CollectorConfig, path: str, *, params: dict[str, Any] | None = None) -> int:
    _, headers = supabase_get(config, path, params=params, range_header="0-0")
    content_range = headers.get("Content-Range", "")
    if "/" not in content_range:
        return 0
    try:
        return int(content_range.split("/")[-1])
    except ValueError:
        return 0


def parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def fetch_latest_boss_check(config: CollectorConfig) -> datetime | None:
    rows, _ = supabase_get(
        config,
        "worker_coordination_messages",
        params={
            "select": "created_at",
            "worker_name": f"eq.{COORDINATOR_NAME}",
            "message_type": "eq.status",
            "order": "created_at.desc",
            "limit": 1,
        },
    )
    if isinstance(rows, list) and rows:
        return parse_iso_timestamp(rows[0].get("created_at"))
    return None


def fetch_global_collection_summary(config: CollectorConfig) -> dict[str, Any]:
    total_collected = 0
    total_goal = 0
    offset = 0
    while True:
        rows, _ = supabase_get(
            config,
            "collection_targets",
            params={"select": "collected_count,target_goal", "order": "target_goal.asc", "limit": DEFAULT_PAGE_SIZE, "offset": offset},
        )
        if not rows:
            break
        for row in rows:
            try:
                total_collected += int(row.get("collected_count") or 0)
                total_goal += int(row.get("target_goal") or 0)
            except (TypeError, ValueError):
                continue
        if len(rows) < DEFAULT_PAGE_SIZE:
            break
        offset += DEFAULT_PAGE_SIZE
    pct = 0.0 if total_goal == 0 else (total_collected / total_goal) * 100
    return {
        "collected": total_collected,
        "goal": total_goal,
        "pct": round(pct, 4),
    }


def fetch_import_row_count(config: CollectorConfig) -> int:
    return get_exact_count(config, "import_rows", params={"select": "id"})


def fetch_latest_import_row_time(config: CollectorConfig) -> str | None:
    rows, _ = supabase_get(
        config,
        "import_rows",
        params={"select": "created_at", "order": "created_at.desc", "limit": 1},
    )
    if isinstance(rows, list) and rows:
        return rows[0].get("created_at")
    return None


def fetch_recent_batches(config: CollectorConfig, since: datetime | None) -> list[dict[str, Any]]:
    params = {
        "select": "source_name,target_state,target_county,target_category,accepted_rows,created_at",
        "order": "created_at.asc",
    }
    rows, _ = supabase_get(config, "import_batches", params=params)
    if not isinstance(rows, list):
        return []
    if since is None:
        return rows
    recent: list[dict[str, Any]] = []
    for row in rows:
        created_at = parse_iso_timestamp(row.get("created_at"))
        if created_at and created_at > since:
            recent.append(row)
    return recent


def fetch_recent_workers(config: CollectorConfig, since_minutes: int) -> list[str]:
    cutoff = datetime.now(timezone.utc).timestamp() - since_minutes * 60
    rows, _ = supabase_get(
        config,
        "worker_coordination_messages",
        params={
            "select": "worker_name,created_at",
            "order": "created_at.desc",
            "limit": 200,
        },
    )
    workers: list[str] = []
    if isinstance(rows, list):
        for row in rows:
            created_at = parse_iso_timestamp(row.get("created_at"))
            if not created_at:
                continue
            if created_at.timestamp() >= cutoff:
                name = (row.get("worker_name") or "").strip()
                if name and name not in workers:
                    workers.append(name)
    return workers


def fetch_pending_targets(config: CollectorConfig) -> list[dict[str, Any]]:
    state_pool = [state for state in US_51_JURISDICTIONS if state != AR_STATE]
    allowed_niches = parse_collect_niches()
    payload_candidates: list[dict[str, Any]] = [
        {"states": state_pool, "batch_limit": max(1, config.burst_limit)},
        {"states": state_pool, "limit": max(1, config.burst_limit)},
        {"p_states": state_pool, "p_limit": max(1, config.burst_limit)},
        {"p_states": state_pool, "batch_limit": max(1, config.burst_limit)},
        {"p_states": state_pool, "p_batch_limit": max(1, config.burst_limit)},
        {"arg1": state_pool, "arg2": max(1, config.burst_limit)},
        {"state_codes": state_pool, "batch_limit": max(1, config.burst_limit)},
        {"target_states": state_pool, "batch_limit": max(1, config.burst_limit)},
        {"states": state_pool, "batch_size": max(1, config.burst_limit)},
    ]
    last_error: Exception | None = None
    for payload in payload_candidates:
        try:
            rows = supabase_rpc(config, TEAM_B_NEXT_TARGET_RPC, payload)
            if isinstance(rows, dict) and "data" in rows:
                rows = rows["data"]
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict) and niche_allowed(str(row.get("category_slug") or ""), allowed_niches)]
            if isinstance(rows, dict):
                return [rows] if niche_allowed(str(rows.get("category_slug") or ""), allowed_niches) else []
            return []
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            message = str(exc).lower()
            if "unknown parameter" in message or "invalid input" in message or "400" in message:
                continue
            if "406" in message:
                continue
            continue
    return build_local_fallback_targets(config, last_error)


def build_local_fallback_targets(config: CollectorConfig, rpc_error: Exception | None = None) -> list[dict[str, Any]]:
    allowed_niches = parse_collect_niches()
    if allowed_niches:
        categories = [
            category
            for category in allowed_niches
            if uses_npi_adapter(category) or uses_osm_fallback(category)
        ]
    else:
        categories = sorted(TEAM_B_HEALTHCARE_CATEGORIES)
    targets: list[dict[str, Any]] = []
    for state in [state for state in US_51_JURISDICTIONS if state != AR_STATE]:
        for category in categories:
            targets.append(
                {
                    "state": state,
                    "county": f"{state} fallback county",
                    "category_slug": category,
                    "target_goal": 1,
                    "collected_count": 0,
                    "cycle_status": "pending",
                    "notes": f"Fallback target created locally after RPC failure: {rpc_error}" if rpc_error else "Fallback target created locally.",
                    "metadata": {"fallback": True, "source": "local_state_rotation"},
                }
            )
    return targets


def target_is_active(row: dict[str, Any]) -> bool:
    status = normalize_category(str(row.get("cycle_status") or ""))
    return status not in {"completed", "done", "closed"}


def target_rank(
    row: dict[str, Any],
    worker_slot: int,
    rotation_offset: int = 0,
) -> tuple[int, int, int, int, str, str, str]:
    state = normalize_state(row.get("state"))
    category = normalize_category(row.get("category_slug"))
    county = (row.get("county") or "").strip().lower()
    goal = int(row.get("target_goal") or 0)
    collected = int(row.get("collected_count") or 0)
    gap = max(goal - collected, 0)
    bucket = stable_bucket(f"{state}:{county}:{category}:{rotation_offset}", 8)
    slot_distance = (bucket - ((worker_slot - 1) % 8)) % 8
    return (
        state_priority(state),
        category_priority(category),
        slot_distance,
        -gap,
        state,
        county,
        category,
    )


def choose_next_target(
    targets: list[dict[str, Any]],
    worker_slot: int,
    rotation_offset: int = 0,
) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    allowed_niches = parse_collect_niches()
    active_targets = [
        row
        for row in targets
        if target_is_active(row)
        and not target_pair_is_recent(normalize_state(row.get("state")), normalize_category(row.get("category_slug")), now)
        and niche_allowed(str(row.get("category_slug") or ""), allowed_niches)
    ]
    if not active_targets:
        active_targets = [
            row
            for row in targets
            if target_is_active(row)
            and not target_pair_is_recent(
                normalize_state(row.get("state")),
                normalize_category(row.get("category_slug")),
                now,
                TARGET_PAIR_REPAIR_WINDOW,
            )
            and niche_allowed(str(row.get("category_slug") or ""), allowed_niches)
        ]
    if osm_lane_paused(now):
        active_targets = [row for row in active_targets if source_family_for_category(str(row.get("category_slug") or "")) != "osm_fallback"]
    if not active_targets:
        return None
    active_targets.sort(key=lambda row: target_rank(row, worker_slot, rotation_offset))
    return active_targets[0]


def extract_city_hint(metadata: Any) -> str | None:
    if isinstance(metadata, dict):
        for key in ("city", "city_name", "municipality"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def collect_npi_batch(config: CollectorConfig, target: dict[str, Any]) -> dict[str, Any]:
    state = normalize_state(target.get("state"))
    county = (target.get("county") or "").strip()
    category = normalize_category(target.get("category_slug"))
    metadata = target.get("metadata")
    city_hint = extract_city_hint(metadata)

    adapter = Path("adapters") / "npi_registry_adapter.py"
    if not adapter.exists():
        return {"status": "blocked", "reason": "npi_adapter_missing", "state": state, "county": county, "category": category}

    cmd = [
        sys.executable,
        str(adapter),
        "collect",
        "--state",
        state,
        "--county",
        county,
        "--category",
        category,
        "--max-rows",
        os.environ.get("TEAM_B_BATCH_MAX_ROWS", "10"),
        "--insert",
        "--worker-name",
        f"team-b-slot-{config.worker_slot}",
    ]
    if city_hint:
        cmd.extend(["--city", city_hint])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    result: dict[str, Any] = {
        "adapter": "npi_registry",
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "state": state,
        "county": county,
        "category": category,
        "city_hint": city_hint,
    }
    if proc.returncode != 0:
        result["status"] = "error"
        return result
    try:
        parsed = json.loads(proc.stdout)
    except json.JSONDecodeError:
        parsed = {"raw_output": proc.stdout.strip()}
    result["status"] = "ok"
    result["adapter_result"] = parsed
    return result


def closeout_target(
    config: CollectorConfig,
    target: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    if is_synthetic_target(target):
        return {
            "state": normalize_state(target.get("state")),
            "county": (target.get("county") or "").strip(),
            "category": normalize_category(target.get("category_slug")),
            "target_signature": target_signature(target),
            "skipped": True,
            "reason": "synthetic_fallback_target_has_no_collection_targets_row",
        }
    state = normalize_state(target.get("state"))
    county = (target.get("county") or "").strip()
    category = normalize_category(target.get("category_slug"))
    goal = int(target.get("target_goal") or 1)
    collected = int(target.get("collected_count") or 0)
    adapter_result = result.get("adapter_result") if isinstance(result, dict) else {}
    durable_result = adapter_result.get("durable_result") if isinstance(adapter_result, dict) else {}
    inserted_count = int((durable_result or {}).get("inserted_count") or 0)
    rows_collected = int(((adapter_result or {}).get("summary") or {}).get("rows_collected") or 0)
    progress = max(collected, 0) + max(inserted_count, rows_collected, 1)
    update_body = {
        "cycle_status": "completed",
        "collected_count": max(goal, progress),
    }
    _, headers = supabase_patch(
        config,
        "collection_targets",
        update_body,
        params={
            "state": f"eq.{state}",
            "county": f"eq.{county}",
            "category_slug": f"eq.{category}",
        },
    )
    return {
        "state": state,
        "county": county,
        "category": category,
        "target_signature": target_signature(target),
        "update": update_body,
        "content_range": headers.get("Content-Range"),
    }


def collect_non_npi_batch(config: CollectorConfig, target: dict[str, Any]) -> dict[str, Any]:
    state = normalize_state(target.get("state"))
    county = (target.get("county") or "").strip()
    category = normalize_category(target.get("category_slug"))
    family = source_family_for_category(category)
    remember_non_osm_attempt(state, category, family)
    if family == "osm_fallback":
        return collect_osm_fallback_batch(config, target)
    return {
        "status": "blocked",
        "reason": "adapter_not_implemented_yet",
        "source_family": family,
        "state": state,
        "county": county,
        "category": category,
        "next_source": family,
    }


def osm_query_term_for_category(category_slug: str) -> str:
    category = normalize_category(category_slug)
    overrides = {
        "event-planners": "event planner",
        "wedding-planners": "wedding planner",
        "party-planners": "party planner",
        "event-coordinators": "event coordinator",
        "djs": "dj",
        "entertainment-services": "entertainment service",
        "party-rental-companies": "party rental",
        "event-equipment-rental": "event equipment rental",
        "photo-booth-rental": "photo booth rental",
        "wedding-venues": "wedding venue",
        "event-venues": "event venue",
        "retail-stores": "retail store",
        "clothing-stores": "clothing store",
        "shoe-stores": "shoe store",
        "jewelry-stores": "jewelry store",
        "furniture-stores": "furniture store",
        "home-decor-stores": "home decor store",
        "convenience-stores": "convenience store",
        "specialty-retail": "specialty retail",
        "e-commerce-sellers": "online retailer",
        "restaurants": "restaurant",
        "cafe": "cafe",
        "cafes": "cafe",
        "coffee-shops": "coffee shop",
        "food-trucks": "food truck",
        "bakeries": "bakery",
        "catering-companies": "catering",
        "juice-bars": "juice bar",
        "ice-cream-shops": "ice cream shop",
        "photographers": "photographer",
        "videographers": "videographer",
        "video-editors": "video editor",
        "graphic-designers": "graphic designer",
        "content-creators": "content creator",
        "podcast-producers": "podcast producer",
    }
    return overrides.get(category, category.replace("-", " "))


def normalize_osm_result(result: dict[str, Any], *, category: str, state: str, county: str, query_term: str) -> dict[str, Any] | None:
    osm_type = normalize_category(str(result.get("osm_type") or ""))
    osm_id = str(result.get("osm_id") or "").strip()
    if not osm_type or not osm_id:
        return None
    address = result.get("address") or {}
    extratags = result.get("extratags") or {}
    name = normalize_spaces(result.get("name") or "")
    display_name = normalize_spaces(result.get("display_name") or "")
    if not name and display_name:
        name = display_name.split(",")[0].strip()
    if not name:
        return None
    house_number = normalize_spaces(address.get("house_number"))
    road = normalize_spaces(address.get("road") or address.get("pedestrian") or address.get("commercial"))
    address_line1 = normalize_spaces(" ".join(part for part in [house_number, road] if part))
    city = normalize_spaces(
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("hamlet")
        or address.get("municipality")
    ) or None
    postal_code = normalize_spaces(address.get("postcode")) or None
    website_url = normalize_spaces(extratags.get("website")) or None
    phone = normalize_spaces(extratags.get("contact:phone") or extratags.get("phone")) or None
    email = normalize_spaces(extratags.get("contact:email") or extratags.get("email")) or None
    source_url = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
    normalized_name = normalize_name_key(name)
    normalized_address = normalize_address_key(address_line1, city, state, postal_code)
    return {
        "business_name": name,
        "normalized_business_name": normalized_name,
        "phone": phone,
        "website_url": website_url,
        "email": email,
        "address_line1": address_line1 or None,
        "city": city,
        "postal_code": postal_code,
        "source_url": source_url,
        "notes": PRIVATE_REVIEW_NOTE,
        "private_review_required": True,
        "publication_status": "private_review_required",
        "source_identifier": f"osm:{osm_type}:{osm_id}",
        "category_slug": category,
        "source_name": SOURCE_NAME_OSM_FALLBACK,
        "source_lane_code": OSM_NOMINATIM_SOURCE_LANE,
        "state": state,
        "taxonomy_code": None,
        "taxonomy_description": None,
        "license_number": None,
        "license_state": None,
        "provider_status": None,
        "enumeration_type": None,
        "enumeration_date": None,
        "last_updated": None,
        "address_purpose": "LOCATION",
        "dedupe_keys": {
            "source_identifier": f"osm:{osm_type}:{osm_id}",
            "normalized_business_name": normalized_name,
            "phone": phone,
            "normalized_address": normalized_address,
            "website_url": website_url,
            "source_url": source_url,
        },
        "metadata": {
            "query_term": query_term,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "county": county,
        },
    }


def normalize_photon_result(result: dict[str, Any], *, category: str, state: str, county: str, query_term: str) -> dict[str, Any] | None:
    properties = result.get("properties") or {}
    geometry = result.get("geometry") or {}
    coordinates = geometry.get("coordinates") or []
    osm_type = normalize_category(str(properties.get("osm_type") or ""))
    osm_id = str(properties.get("osm_id") or result.get("id") or "").strip()
    if not osm_id:
        return None
    name = normalize_spaces(properties.get("name") or properties.get("street") or "")
    if not name and isinstance(properties.get("extent"), list):
        name = normalize_spaces(properties.get("city") or properties.get("state") or "")
    if not name:
        name = normalize_spaces(properties.get("city") or properties.get("street") or "")
    if not name:
        return None
    street = normalize_spaces(" ".join(
        part for part in [properties.get("housenumber"), properties.get("street")] if normalize_spaces(str(part or ""))
    ))
    city = normalize_spaces(properties.get("city") or properties.get("state") or "") or None
    postal_code = normalize_spaces(properties.get("postcode")) or None
    website_url = normalize_spaces(properties.get("website")) or None
    phone = normalize_spaces(properties.get("phone")) or None
    email = normalize_spaces(properties.get("email")) or None
    source_url = f"https://www.openstreetmap.org/{osm_type or 'node'}/{osm_id}"
    normalized_name = normalize_name_key(name)
    normalized_address = normalize_address_key(street, city, state, postal_code)
    return {
        "business_name": name,
        "normalized_business_name": normalized_name,
        "phone": phone,
        "website_url": website_url,
        "email": email,
        "address_line1": street or None,
        "city": city,
        "postal_code": postal_code,
        "source_url": source_url,
        "notes": PRIVATE_REVIEW_NOTE,
        "private_review_required": True,
        "publication_status": "private_review_required",
        "source_identifier": f"photon:{osm_type or 'feature'}:{osm_id}",
        "category_slug": category,
        "source_name": SOURCE_NAME_OSM_FALLBACK,
        "source_lane_code": OSM_PHOTON_SOURCE_LANE,
        "state": state,
        "taxonomy_code": None,
        "taxonomy_description": None,
        "license_number": None,
        "license_state": None,
        "provider_status": None,
        "enumeration_type": None,
        "enumeration_date": None,
        "last_updated": None,
        "address_purpose": "LOCATION",
        "dedupe_keys": {
            "source_identifier": f"photon:{osm_type or 'feature'}:{osm_id}",
            "normalized_business_name": normalized_name,
            "phone": phone,
            "normalized_address": normalized_address,
            "website_url": website_url,
            "source_url": source_url,
        },
        "metadata": {
            "query_term": query_term,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "county": county,
            "coordinates": coordinates,
        },
    }


def collect_osm_fallback_batch(config: CollectorConfig, target: dict[str, Any]) -> dict[str, Any]:
    state = normalize_state(target.get("state"))
    county = (target.get("county") or "").strip()
    category = normalize_category(target.get("category_slug"))
    if osm_lane_paused():
        return {
            "status": "paused",
            "reason": "osm_lane_paused_until_non_osm_attempt_recurs",
            "source_family": "osm_fallback",
            "state": state,
            "county": county,
            "category": category,
        }
    query_term = osm_query_term_for_category(category)
    search_contexts = [
        county if "fallback" not in county.lower() else "",
        "",
    ]
    rows: list[dict[str, Any]] = []
    used_query = ""
    for search_county in search_contexts:
        query = " ".join(part for part in [query_term, search_county, state, "USA"] if part)
        used_query = query

        photon_params = {
            "q": query,
            "limit": str(OSM_FALLBACK_LIMIT),
            "lang": "en",
        }
        photon_url = f"{PHOTON_SEARCH_URL}?{urllib.parse.urlencode(photon_params)}"
        try:
            photon_payload = request_json(photon_url)
        except AdapterError:
            photon_payload = None
        if isinstance(photon_payload, dict):
            features = photon_payload.get("features") or []
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                normalized = normalize_photon_result(feature, category=category, state=state, county=county, query_term=query_term)
                if normalized:
                    rows.append(normalized)
        if rows:
            break

        nominatim_params = {
            "format": "jsonv2",
            "q": query,
            "countrycodes": "us",
            "limit": str(OSM_FALLBACK_LIMIT),
            "addressdetails": "1",
            "namedetails": "1",
            "extratags": "1",
        }
        nominatim_url = f"{OSM_NOMINATIM_SEARCH_URL}?{urllib.parse.urlencode(nominatim_params)}"
        try:
            payload = request_json(nominatim_url)
        except AdapterError:
            payload = None
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                normalized = normalize_osm_result(item, category=category, state=state, county=county, query_term=query_term)
                if normalized:
                    rows.append(normalized)
        if rows:
            break
    deduped = dedupe_rows(rows)[:OSM_FALLBACK_LIMIT]
    if not deduped:
        return {
            "status": "blocked",
            "reason": "osm_no_results",
            "source_family": "osm_fallback",
            "state": state,
            "county": county,
            "category": category,
            "query": used_query,
        }
    client_body = {
        "p_worker_name": f"team-b-slot-{state.lower()}",
        "p_state": state,
        "p_county": county,
        "p_category_slug": category,
        "p_source_name": SOURCE_NAME_OSM_FALLBACK,
        "p_source_url": f"{PHOTON_SEARCH_URL}?{urllib.parse.urlencode({'q': used_query, 'limit': str(OSM_FALLBACK_LIMIT), 'lang': 'en'})}",
        "p_rows": deduped,
        "p_note": "OSM fallback batch. Private review required before publication.",
        "p_payload": {
            "adapter": "continuous_collector_osm_fallback",
            "private_review_required": True,
            "query": used_query,
            "query_term": query_term,
            "county": county,
            "state": state,
        },
    }
    durable_result = supabase_rpc(config, "agenarys_durable_record_collection", client_body)
    return {
        "status": "ok",
        "source": SOURCE_NAME_OSM_FALLBACK,
        "adapter": "osm_fallback",
        "state": state,
        "county": county,
        "category": category,
        "query": query,
        "summary": {
            "state": state,
            "county": county,
            "category": category,
            "rows_collected": len(deduped),
        },
        "sample_rows": deduped[: min(3, len(deduped))],
        "durable_result": durable_result,
    }


def record_status_message(config: CollectorConfig, summary: dict[str, Any]) -> None:
    body = {
        "lane": "Team B",
        "summary": summary,
    }
    return


def reset_stale_targets(config: CollectorConfig, recent_batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not recent_batches:
        return []
    active_pairs = {
        (
            normalize_state(row.get("target_state")),
            normalize_category(row.get("target_category")),
        )
        for row in recent_batches
    }
    rows, _ = supabase_get(
        config,
        "collection_targets",
        params={
            "select": "state,county,category_slug,cycle_status",
            "category_slug": f"in.({','.join(TEAM_B_CATEGORY_SLUGS)})",
            "cycle_status": "in.(claimed,in_progress,running,locked)",
            "limit": 5000,
        },
    )
    if not isinstance(rows, list):
        return []
    reset_rows: list[dict[str, Any]] = []
    for row in rows:
        state = normalize_state(row.get("state"))
        category = normalize_category(row.get("category_slug"))
        if (state, category) in active_pairs:
            continue
        supabase_patch(
            config,
            "collection_targets",
            {"cycle_status": "pending"},
            params={
                "state": f"eq.{state}",
                "county": f"eq.{row.get('county')}",
                "category_slug": f"eq.{row.get('category_slug')}",
            },
        )
        reset_rows.append(row)
    return reset_rows


def fetch_last_dispatch_time(config: CollectorConfig) -> datetime | None:
    return None


def should_dispatch_awaken_swarm(config: CollectorConfig, pending_exists: bool) -> tuple[bool, str]:
    if not pending_exists:
        return False, "no_pending_work"
    if not config.github_token or not config.github_repository:
        return False, "missing_github_token_or_repository"
    if config.worker_slot != 1:
        return False, "non_coordinator_slot"
    return True, "dispatch_due"


def dispatch_awaken_swarm(config: CollectorConfig, summary: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "event_type": TEAM_B_DISPATCH_EVENT,
        "client_payload": {
            "lane": "Team B",
            "worker_slot": config.worker_slot,
            "latest_row_time": summary.get("latest_row_time"),
            "rows_added_since_last_check": summary.get("rows_added_since_last_check"),
            "pct": summary.get("official_collection_pct"),
        },
    }
    url = f"https://api.github.com/repos/{config.github_repository}/dispatches"
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {config.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return {"status": response.status, "reason": response.reason}


def boss_report(config: CollectorConfig) -> dict[str, Any]:
    latest_boss_check = fetch_latest_boss_check(config)
    collection_summary = fetch_global_collection_summary(config)
    total_import_rows = fetch_import_row_count(config)
    latest_row_time = fetch_latest_import_row_time(config)
    live_log = resolve_live_log_path()
    recent_batches = fetch_recent_batches(config, latest_boss_check)

    rows_added_since_last_check = sum(int(row.get("accepted_rows") or 0) for row in recent_batches)
    states_moving = sorted({normalize_state(row.get("target_state")) for row in recent_batches if row.get("target_state")})
    categories_moving = sorted({normalize_category(row.get("target_category")) for row in recent_batches if row.get("target_category")})
    adapters_moving = sorted({row.get("source_name") for row in recent_batches if row.get("source_name")})
    recent_workers_15 = fetch_recent_workers(config, 15)
    recent_workers_60 = fetch_recent_workers(config, 60)
    reset_rows = reset_stale_targets(config, recent_batches)

    pending_targets = fetch_pending_targets(config)
    next_target = choose_next_target(pending_targets, config.worker_slot)
    next_source = source_family_for_category(next_target.get("category_slug")) if next_target else None

    stale_locks = []
    for target in pending_targets:
        if normalize_category(target.get("cycle_status")) in {"claimed", "in_progress", "running", "locked"}:
            stale_locks.append(
                {
                    "state": normalize_state(target.get("state")),
                    "county": target.get("county"),
                    "category": normalize_category(target.get("category_slug")),
                }
            )

    report = {
        "official_collection_pct": collection_summary["pct"],
        "raw_import_pct": round((total_import_rows / collection_summary["goal"] * 100) if collection_summary["goal"] else 0.0, 4),
        "total_import_rows": total_import_rows,
        "rows_added_since_last_check": rows_added_since_last_check,
        "latest_row_time": latest_row_time,
        "states_moving": states_moving,
        "categories_moving": categories_moving,
        "active_adapters": adapters_moving,
        "workers_last_15m": recent_workers_15,
        "workers_last_60m": recent_workers_60,
        "adapter_starts_since_last_check": len(recent_batches),
        "stale_locks": stale_locks,
        "stale_locks_reset": len(reset_rows),
        "next_target": next_target,
        "next_source": next_source,
        "latest_boss_check": latest_boss_check.isoformat() if latest_boss_check else None,
        "pending_work_exists": bool(pending_targets),
        "live_log_path": live_log["path"],
        "live_log_rebound": live_log["rebound"],
        "live_log_stale": live_log["stale"],
        "live_log_age_hours": live_log["age_hours"],
    }
    return report


def run_collection(config: CollectorConfig, report: dict[str, Any], rotation_offset: int = 0) -> list[dict[str, Any]]:
    pending_targets = fetch_pending_targets(config)
    if not pending_targets:
        return []
    now = datetime.now(timezone.utc)
    targets = filter_target_pairs_by_revisit_window(pending_targets, now, TARGET_PAIR_REVISIT_WINDOW)
    if not targets:
        targets = filter_target_pairs_by_revisit_window(pending_targets, now, TARGET_PAIR_REPAIR_WINDOW)
    if not targets:
        return []
    targets.sort(key=lambda row: target_rank(row, config.worker_slot, rotation_offset))
    results: list[dict[str, Any]] = []
    for target in targets:
        if len(results) >= config.burst_limit:
            break
        category = normalize_category(target.get("category_slug"))
        family = source_family_for_category(category)
        if uses_osm_fallback(category) and osm_lane_paused():
            continue
        print_json(
            "team_b_cycle_target",
            {
                "target_signature": target_signature(target),
                "state": normalize_state(target.get("state")),
                "county": (target.get("county") or "").strip(),
                "category": category,
                "source_family": family,
                "rotation_offset": rotation_offset,
            },
        )
        if uses_npi_adapter(category):
            result = collect_npi_batch(config, target)
        elif uses_osm_fallback(category):
            result = collect_osm_fallback_batch(config, target)
        else:
            result = collect_non_npi_batch(config, target)
        results.append(result)
        remember_target_pair(normalize_state(target.get("state")), category)
        if result.get("status") == "ok":
            try:
                print_json("team_b_target_closeout", closeout_target(config, target, result))
            except Exception as exc:  # noqa: BLE001
                print_json(
                    "team_b_target_closeout_failed",
                    {
                        "target_signature": target_signature(target),
                        "error": str(exc),
                    },
                )
    return results


def print_json(label: str, payload: Any) -> None:
    print(json.dumps({"label": label, "payload": payload}, indent=2, sort_keys=True))


def normalize_supabase_url(raw: str) -> str:
    value = raw.strip().rstrip("/")
    if value.startswith("//"):
        return f"https:{value}"
    if value and "://" not in value:
        return f"https://{value}"
    return value


def build_config(args: argparse.Namespace) -> CollectorConfig:
    return CollectorConfig(
        supabase_url=normalize_supabase_url(os.environ.get("SUPABASE_URL", "")),
        service_role_key=os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        github_token=os.environ.get("GITHUB_TOKEN", ""),
        github_repository=os.environ.get("GITHUB_REPOSITORY", ""),
        worker_slot=parse_worker_slot(os.environ.get("WORKER_SLOT")),
        boss_check=os.environ.get("TEAM_B_BOSS_CHECK", "0") == "1" or args.boss_check,
        burst_limit=max(1, args.burst_limit),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Team B continuous collector for Agenarys")
    parser.add_argument("--boss-check", action="store_true", help="Force a boss check report in addition to collection")
    parser.add_argument("--burst-limit", type=int, default=DEFAULT_BURST_LIMIT, help="Maximum targets to try in this run")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = build_config(args)
    load_target_pair_history()
    load_non_osm_attempt_state()

    if not config.supabase_url or not config.service_role_key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")

    if config.boss_check:
        print_json("team_b_boss_check", {"status": "disabled_for_public_rpc_path"})

    cycle = 0
    empty_queue_streak = 0
    while True:
        cycle += 1
        started_at = datetime.now(timezone.utc).isoformat()
        pending_targets = None
        collection_results = []
        sleep_seconds = max(1, DEFAULT_CONTINUOUS_SLEEP_SECONDS)
        try:
            pending_targets = fetch_pending_targets(config)
            collection_results = run_collection(config, {"pending_targets": pending_targets}, rotation_offset=cycle - 1)
            if collection_results:
                empty_queue_streak = 0
                print_json(
                    "team_b_collection_results",
                    {
                        "cycle": cycle,
                        "started_at": started_at,
                        "results": collection_results,
                    },
                )
            else:
                empty_queue_streak += 1
                print_json(
                    "team_b_collection_results",
                    {
                        "cycle": cycle,
                        "started_at": started_at,
                        "status": "no_pending_targets",
                        "empty_queue_streak": empty_queue_streak,
                    },
                )
                sleep_seconds = max(30, min(30 * max(empty_queue_streak, 1), 300))
        except Exception as exc:  # noqa: BLE001
            empty_queue_streak += 1
            sleep_seconds = max(30, min(30 * max(empty_queue_streak, 1), 300))
            print_json(
                "team_b_cycle_error",
                {
                    "cycle": cycle,
                    "started_at": started_at,
                    "error": str(exc),
                },
            )
        time.sleep(sleep_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
