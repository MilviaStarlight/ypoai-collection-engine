#!/usr/bin/env python3
"""
NPI Registry source adapter for Agenarys private-review collection.

Streams public NPPES NPI Registry rows into the existing durable Supabase RPC:
public.agenarys_durable_record_collection

The adapter intentionally does not store raw datasets locally.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable

import requests


NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"
NPI_API_VERSION = "2.1"
SOURCE_NAME = "NPPES NPI Registry API"
PRIVATE_REVIEW_NOTE = "Private review required before publication."
DEFAULT_WORKER_NAME = "agenarys-npi-registry-adapter"
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 200
SUPABASE_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("TEAM_B_SUPABASE_TIMEOUT_SECONDS", "180"))

US_51_JURISDICTIONS = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY",
]

CATEGORY_QUERIES: dict[str, list[dict[str, str]]] = {
    "doctor": [
        {"taxonomy_description": "Family Medicine"},
        {"taxonomy_description": "Internal Medicine"},
        {"taxonomy_description": "Pediatrics"},
        {"taxonomy_description": "Obstetrics & Gynecology"},
        {"taxonomy_description": "Emergency Medicine"},
        {"taxonomy_description": "Dermatology"},
        {"taxonomy_description": "Orthopaedic Surgery"},
        {"taxonomy_description": "Ophthalmology"},
        {"taxonomy_description": "Otolaryngology"},
        {"taxonomy_description": "Urology"},
    ],
    "dentist": [
        {"taxonomy_description": "Dentist"},
    ],
    "medical-services": [
        {"taxonomy_description": "Clinic/Center", "enumeration_type": "NPI-2"},
    ],
    "urgent-care": [
        {"taxonomy_description": "Urgent Care"},
    ],
    "physical-therapy": [
        {"taxonomy_description": "Physical Therapist"},
        {"taxonomy_description": "Clinic/Center, Physical Therapy", "enumeration_type": "NPI-2"},
    ],
    "chiropractors": [
        {"taxonomy_description": "Chiropractor"},
    ],
    "pharmacies": [
        {"taxonomy_description": "Pharmacy"},
    ],
    "optometrists": [
        {"taxonomy_description": "Optometrist"},
    ],
    "veterinarians": [
        {"taxonomy_description": "Veterinarian"},
    ],
    "medical-clinics": [
        {"taxonomy_description": "Clinic/Center", "enumeration_type": "NPI-2"},
    ],
    "mental-health-clinics": [
        {"taxonomy_description": "Clinic/Center", "enumeration_type": "NPI-2"},
    ],
}


class AdapterError(RuntimeError):
    pass


@dataclass(frozen=True)
class NpiQuery:
    state: str
    category: str
    taxonomy_description: str
    enumeration_type: str | None = None
    city: str | None = None
    postal_code: str | None = None
    limit: int = DEFAULT_PAGE_SIZE
    skip: int = 0

    def params(self) -> dict[str, str]:
        params = {
            "version": NPI_API_VERSION,
            "country_code": "US",
            "address_purpose": "LOCATION",
            "state": self.state.upper(),
            "taxonomy_description": self.taxonomy_description,
            "limit": str(min(self.limit, MAX_PAGE_SIZE)),
            "skip": str(max(self.skip, 0)),
        }
        if self.city:
            params["city"] = self.city
        if self.postal_code:
            params["postal_code"] = self.postal_code
        if self.enumeration_type:
            params["enumeration_type"] = self.enumeration_type
        return params

    def url(self) -> str:
        return NPI_API_URL + "?" + urllib.parse.urlencode(self.params())


def request_json(url: str, timeout_seconds: int = 30, retries: int = 3) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AgenarysNPIAdapter/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(1.5 * (attempt + 1))
    raise AdapterError(f"Request failed after {retries} attempts: {url} :: {last_error}")


def normalize_spaces(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name_key(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", "", normalize_spaces(value).upper())


def normalize_phone(value: str | None) -> str | None:
    digits = re.sub(r"\D+", "", value or "")
    if not digits:
        return None
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
    return digits


def normalize_address_key(*parts: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", "", " ".join(normalize_spaces(p).upper() for p in parts if p))


def title_or_upper(value: str | None) -> str | None:
    cleaned = normalize_spaces(value)
    if not cleaned:
        return None
    if cleaned.isupper():
        return cleaned.title()
    return cleaned


def choose_location_address(result: dict[str, Any]) -> dict[str, Any]:
    addresses = result.get("addresses") or []
    for address in addresses:
        if address.get("address_purpose") == "LOCATION" and address.get("country_code") == "US":
            return address
    for address in addresses:
        if address.get("country_code") == "US":
            return address
    return addresses[0] if addresses else {}


def choose_primary_taxonomy(result: dict[str, Any]) -> dict[str, Any]:
    taxonomies = result.get("taxonomies") or []
    for taxonomy in taxonomies:
        if taxonomy.get("primary") is True:
            return taxonomy
    return taxonomies[0] if taxonomies else {}


def provider_name(result: dict[str, Any]) -> str | None:
    basic = result.get("basic") or {}
    organization = normalize_spaces(basic.get("organization_name"))
    if organization:
        return organization

    pieces = [
        basic.get("name_prefix"),
        basic.get("first_name"),
        basic.get("middle_name"),
        basic.get("last_name"),
        basic.get("name_suffix"),
        basic.get("credential"),
    ]
    name = normalize_spaces(" ".join(str(p) for p in pieces if p))
    return name or None


def source_url_for_npi(npi_number: str) -> str:
    return NPI_API_URL + "?" + urllib.parse.urlencode({"version": NPI_API_VERSION, "number": npi_number})


def normalize_npi_result(result: dict[str, Any], category: str) -> dict[str, Any] | None:
    npi_number = normalize_spaces(str(result.get("number") or ""))
    business_name = provider_name(result)
    address = choose_location_address(result)
    taxonomy = choose_primary_taxonomy(result)
    basic = result.get("basic") or {}

    if not npi_number or not business_name:
        return None

    address_line1 = normalize_spaces(
        " ".join(
            str(part)
            for part in [address.get("address_1"), address.get("address_2")]
            if normalize_spaces(str(part or ""))
        )
    )
    city = title_or_upper(address.get("city"))
    state = normalize_spaces(address.get("state")).upper()
    postal_code = normalize_spaces(address.get("postal_code"))
    phone = normalize_phone(address.get("telephone_number") or basic.get("authorized_official_telephone_number"))
    source_url = source_url_for_npi(npi_number)
    normalized_name = normalize_name_key(business_name)
    normalized_address = normalize_address_key(address_line1, city, state, postal_code)

    return {
        "business_name": business_name,
        "normalized_business_name": normalized_name,
        "phone": phone,
        "website_url": None,
        "email": None,
        "address_line1": address_line1 or None,
        "city": city,
        "postal_code": postal_code or None,
        "source_url": source_url,
        "notes": PRIVATE_REVIEW_NOTE,
        "private_review_required": True,
        "publication_status": "private_review_required",
        "source_identifier": npi_number,
        "npi_number": npi_number,
        "category_slug": category,
        "source_name": SOURCE_NAME,
        "source_lane_code": "npi_registry",
        "state": state or None,
        "taxonomy_code": taxonomy.get("code"),
        "taxonomy_description": taxonomy.get("desc"),
        "license_number": taxonomy.get("license"),
        "license_state": taxonomy.get("state"),
        "provider_status": basic.get("status"),
        "enumeration_type": result.get("enumeration_type"),
        "enumeration_date": basic.get("enumeration_date"),
        "last_updated": basic.get("last_updated"),
        "address_purpose": address.get("address_purpose"),
        "dedupe_keys": {
            "npi": npi_number,
            "normalized_business_name": normalized_name,
            "phone": phone,
            "normalized_address": normalized_address,
            "source_url": source_url,
            "license_number": taxonomy.get("license"),
        },
    }


def dedupe_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        keys = row.get("dedupe_keys") or {}
        preferred_key = ("npi", str(keys.get("npi") or ""))
        fallback_key = (
            "business",
            str(keys.get("normalized_business_name") or ""),
            str(keys.get("phone") or ""),
            str(keys.get("normalized_address") or ""),
            str(row.get("website_url") or ""),
        )
        key = preferred_key if preferred_key[1] else fallback_key
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def category_specs(category: str) -> list[dict[str, str]]:
    try:
        return CATEGORY_QUERIES[category]
    except KeyError as exc:
        allowed = ", ".join(sorted(CATEGORY_QUERIES))
        raise AdapterError(f"Unsupported category '{category}'. Allowed: {allowed}") from exc


def collect_rows(
    *,
    state: str,
    category: str,
    city: str | None = None,
    postal_code: str | None = None,
    max_rows: int = 50,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages_per_taxonomy: int = 6,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    query_summaries: list[dict[str, Any]] = []
    state = state.upper()

    for spec in category_specs(category):
        total_seen_for_query = 0
        result_count = 0
        for page_index in range(max_pages_per_taxonomy):
            if len(rows) >= max_rows:
                break
            query = NpiQuery(
                state=state,
                city=city,
                postal_code=postal_code,
                category=category,
                taxonomy_description=spec["taxonomy_description"],
                enumeration_type=spec.get("enumeration_type"),
                limit=min(page_size, max_rows - len(rows), MAX_PAGE_SIZE),
                skip=page_index * min(page_size, MAX_PAGE_SIZE),
            )
            payload = request_json(query.url())
            result_count = int(payload.get("result_count") or 0)
            results = payload.get("results") or []
            total_seen_for_query += len(results)
            for item in results:
                normalized = normalize_npi_result(item, category)
                if normalized:
                    rows.append(normalized)
            if not results or len(results) < query.limit:
                break

        query_summaries.append(
            {
                "taxonomy_description": spec["taxonomy_description"],
                "enumeration_type": spec.get("enumeration_type"),
                "result_count": result_count,
                "sampled_rows": total_seen_for_query,
            }
        )

    deduped = dedupe_rows(rows)[:max_rows]
    summary = {
        "state": state,
        "city": city,
        "postal_code": postal_code,
        "category": category,
        "rows_collected": len(deduped),
        "query_summaries": query_summaries,
    }
    return deduped, summary


def estimate_counts(
    states: list[str],
    categories: list[str],
    *,
    sleep_seconds: float = 0.1,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages_per_query: int = 6,
) -> dict[str, Any]:
    estimates: dict[str, Any] = {"source": SOURCE_NAME, "states": {}}
    page_size = min(page_size, MAX_PAGE_SIZE)
    for state in states:
        state = state.upper()
        estimates["states"].setdefault(state, {})
        for category in categories:
            total = 0
            cap_reached = False
            parts: list[dict[str, Any]] = []
            for spec in category_specs(category):
                count = 0
                pages_read = 0
                query_cap_reached = False
                for page_index in range(max_pages_per_query):
                    query = NpiQuery(
                        state=state,
                        category=category,
                        taxonomy_description=spec["taxonomy_description"],
                        enumeration_type=spec.get("enumeration_type"),
                        limit=page_size,
                        skip=page_index * page_size,
                    )
                    payload = request_json(query.url())
                    page_count = int(payload.get("result_count") or 0)
                    count += page_count
                    pages_read += 1
                    if sleep_seconds:
                        time.sleep(sleep_seconds)
                    if page_count < page_size:
                        break
                else:
                    query_cap_reached = True
                    cap_reached = True
                total += count
                parts.append(
                    {
                        "taxonomy_description": spec["taxonomy_description"],
                        "enumeration_type": spec.get("enumeration_type"),
                        "retrievable_rows_observed": count,
                        "pages_read": pages_read,
                        "cap_reached": query_cap_reached,
                    }
                )
            estimates["states"][state][category] = {
                "retrievable_rows_observed": total,
                "cap_reached": cap_reached,
                "note": (
                    "State/category reached the configured page cap; split by city/postal target to collect more."
                    if cap_reached
                    else "Observed count reached the end of the current state/category query."
                ),
                "parts": parts,
            }
    return estimates


class SupabaseDurableClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        if not supabase_url or not service_role_key:
            raise AdapterError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required for inserts.")
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key

    @classmethod
    def from_env(cls) -> "SupabaseDurableClient":
        return cls(
            supabase_url=os.environ.get("SUPABASE_URL", ""),
            service_role_key=os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        )

    def record_collection(
        self,
        *,
        worker_name: str,
        state: str,
        county: str,
        category: str,
        source_url: str,
        rows: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        endpoint = f"{self.supabase_url}/rest/v1/rpc/agenarys_durable_record_collection"
        body = {
            "p_worker_name": worker_name,
            "p_state": state.upper(),
            "p_county": county,
            "p_category_slug": category,
            "p_source_name": SOURCE_NAME,
            "p_source_url": source_url,
            "p_rows": rows,
            "p_note": "NPI Registry adapter batch. Private review required before publication.",
            "p_payload": payload,
        }
        try:
            response = requests.post(
                endpoint,
                headers={
                    "Content-Type": "application/json",
                    "apikey": self.service_role_key,
                    "Authorization": f"Bearer {self.service_role_key}",
                },
                json=body,
                timeout=SUPABASE_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            parsed = response.json()
            if isinstance(parsed, list) and parsed:
                return parsed[0]
            if isinstance(parsed, dict):
                return parsed
            return {"raw_response": parsed}
        except requests.HTTPError as exc:
            response = exc.response
            error_body = response.text if response is not None else str(exc)
            status_code = response.status_code if response is not None else "unknown"
            raise AdapterError(f"Supabase durable insert failed: {status_code} {error_body}") from exc


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_states(value: str) -> list[str]:
    if value.upper() == "ALL":
        return US_51_JURISDICTIONS
    states = [state.upper() for state in parse_csv(value)]
    invalid = [state for state in states if state not in US_51_JURISDICTIONS]
    if invalid:
        raise AdapterError(f"Invalid state/jurisdiction codes: {', '.join(invalid)}")
    return states


def command_collect(args: argparse.Namespace) -> int:
    rows, summary = collect_rows(
        state=args.state,
        category=args.category,
        city=args.city,
        postal_code=args.postal_code,
        max_rows=args.max_rows,
        page_size=args.page_size,
        max_pages_per_taxonomy=args.max_pages_per_taxonomy,
    )
    source_url = NPI_API_URL + "?" + urllib.parse.urlencode(
        {
            "version": NPI_API_VERSION,
            "state": args.state.upper(),
            "city": args.city or "",
            "postal_code": args.postal_code or "",
            "category": args.category,
        }
    )
    result = {
        "mode": "dry_run",
        "source": SOURCE_NAME,
        "summary": summary,
        "sample_rows": rows[: min(3, len(rows))],
    }
    if args.insert:
        client = SupabaseDurableClient.from_env()
        durable_result = client.record_collection(
            worker_name=args.worker_name,
            state=args.state,
            county=args.county,
            category=args.category,
            source_url=source_url,
            rows=rows,
            payload={
                "adapter": "npi_registry_adapter",
                "private_review_required": True,
                "city": args.city,
                "postal_code": args.postal_code,
                "query_summary": summary,
            },
        )
        result["mode"] = "insert"
        result["durable_result"] = durable_result
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def command_estimate(args: argparse.Namespace) -> int:
    states = parse_states(args.states)
    categories = parse_csv(args.categories)
    for category in categories:
        category_specs(category)
    estimates = estimate_counts(
        states,
        categories,
        sleep_seconds=args.sleep_seconds,
        page_size=args.page_size,
        max_pages_per_query=args.max_pages_per_query,
    )
    print(json.dumps(estimates, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Agenarys NPI Registry source adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Collect one small NPI batch")
    collect.add_argument("--state", required=True, help="State or DC code")
    collect.add_argument("--county", required=True, help="County name for durable target accounting")
    collect.add_argument("--city", help="Optional city filter")
    collect.add_argument("--postal-code", help="Optional postal code filter")
    collect.add_argument("--category", required=True, choices=sorted(CATEGORY_QUERIES))
    collect.add_argument("--max-rows", type=int, default=25)
    collect.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    collect.add_argument("--max-pages-per-taxonomy", type=int, default=6)
    collect.add_argument("--worker-name", default=DEFAULT_WORKER_NAME)
    collect.add_argument("--insert", action="store_true", help="Insert into Supabase durable RPC")
    collect.add_argument("--dry-run", action="store_true", help="Collect and print summary without inserting")
    collect.set_defaults(func=command_collect)

    estimate = subparsers.add_parser("estimate", help="Estimate rows by state/category without inserting")
    estimate.add_argument("--states", required=True, help="Comma list such as FL,GA or ALL")
    estimate.add_argument(
        "--categories",
        default="doctor,dentist,medical-services,urgent-care,physical-therapy",
        help="Comma list of adapter categories",
    )
    estimate.add_argument("--sleep-seconds", type=float, default=0.1)
    estimate.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    estimate.add_argument("--max-pages-per-query", type=int, default=6)
    estimate.set_defaults(func=command_estimate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "dry_run", False):
        args.insert = False
    try:
        return args.func(args)
    except AdapterError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
