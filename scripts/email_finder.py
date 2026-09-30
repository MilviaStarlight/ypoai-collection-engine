#!/usr/bin/env python3
"""YPOAI email finder.

For each free listing that has a website but no email, visit the business's OWN website (home page plus
its contact / about / team pages), collect the email addresses shown there, and record the best one:

  owner     a named person's address found next to a decision-maker title (Owner, Founder, President, CEO, Dr., ...)
  personal  a named person's address (maria@..., j.rivera@...) with no title nearby
  generic   info@, contact@, office@, hello@, sales@ ... kept only as a fallback

Nothing is guessed and no third-party sites or lists are used. Only pages on the business's own domain are read,
robots.txt is honoured, and one request at a time per site.

Environment: SUPABASE_SERVICE_ROLE_KEY (required), SUPABASE_URL (optional).
Usage: python scripts/email_finder.py [--budget-minutes 300] [--batch 200] [--dry-run URL]
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import worker  # noqa: E402  (Supa client, USER_AGENT)

USER_AGENT = "YPOAI-ContactFinder/1.0 (+https://directory.yellowpagesofai.com/claim; listings@yellowpagesofai.com)"
BROWSER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
FETCH_TIMEOUT = 12
MAX_BYTES = 600_000
MAX_EXTRA_PAGES = 4
PAUSE_BETWEEN_PAGES = 1.0     # seconds, per site
WORKERS = 6                    # different sites in parallel

GENERIC_LOCAL = {
    "info", "contact", "contactus", "hello", "hi", "office", "admin", "sales", "support", "help", "mail", "email",
    "enquiries", "inquiries", "inquiry", "booking", "bookings", "reservations", "orders", "order", "service", "services",
    "team", "staff", "billing", "accounts", "careers", "jobs", "hr", "press", "media", "marketing", "webmaster",
    "noreply", "no-reply", "donotreply", "newsletter", "privacy", "legal", "abuse", "postmaster", "customerservice",
    "customercare", "frontdesk", "reception", "appointments", "scheduling", "general", "questions", "feedback",
}
JUNK_DOMAINS = ("example.com", "sentry.io", "wixpress.com", "wix.com", "squarespace.com", "godaddy.com", "domain.com",
                "email.com", "yourdomain", "sentry-next", "w3.org", "schema.org", "googleapis.com", "cloudflare")
JUNK_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js")
TITLE_RE = re.compile(
    r"\b(owner|owners|co-owner|founder|co-founder|president|ceo|c\.e\.o\.|principal|proprietor|managing (?:partner|director|member)|"
    r"general manager|director|partner|chef[- ]owner|head chef|executive chef|dr\.|doctor|dds|dmd|dvm|md|attorney|esq\.?|broker|"
    r"pastor|superintendent|administrator)\b", re.I)
NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)\b")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
CONTACT_LINK_WORDS = ("contact", "about", "team", "staff", "our-story", "ourstory", "meet", "owner", "leadership",
                      "who-we-are", "people", "doctors", "providers", "attorneys", "agents", "management")


def log(event: str, **fields: Any) -> None:
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": event, **fields}, ensure_ascii=False), flush=True)


class Site:
    """Polite fetcher for one business website."""

    def __init__(self, url: str) -> None:
        self.start = url if re.match(r"^https?://", url, re.I) else "https://" + url
        p = urlparse(self.start)
        self.host = p.netloc.lower()
        self.base = f"{p.scheme}://{p.netloc}"
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
        self.robots = robotparser.RobotFileParser()
        try:
            r = self.s.get(urljoin(self.base, "/robots.txt"), timeout=FETCH_TIMEOUT)
            self.robots.parse(r.text.splitlines() if r.status_code == 200 else [])
        except Exception:  # noqa: BLE001
            self.robots.parse([])

    def allowed(self, url: str) -> bool:
        try:
            return self.robots.can_fetch(USER_AGENT, url)
        except Exception:  # noqa: BLE001
            return True

    def fetch(self, url: str) -> str | None:
        if not self.allowed(url):
            return None
        try:
            with self.s.get(url, timeout=FETCH_TIMEOUT, stream=True, allow_redirects=True) as r:
                if r.status_code == 403 and self.s.headers.get("User-Agent") == USER_AGENT:
                    # Some site firewalls refuse unfamiliar crawlers outright; retry once looking like a browser.
                    self.s.headers["User-Agent"] = BROWSER_AGENT
                    return self.fetch(url)
                if r.status_code != 200 or "html" not in (r.headers.get("Content-Type") or ""):
                    return None
                buf = b""
                for chunk in r.iter_content(65536):
                    buf += chunk
                    if len(buf) > MAX_BYTES:
                        break
                return buf.decode(r.encoding or "utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return None


def cf_decode(hexstr: str) -> str | None:
    """Cloudflare email obfuscation: hex string, first byte is the XOR key."""
    try:
        raw = bytes.fromhex(hexstr)
        return "".join(chr(b ^ raw[0]) for b in raw[1:])
    except Exception:  # noqa: BLE001
        return None


def deobfuscate(text: str) -> str:
    t = html.unescape(text)
    t = re.sub(r"\s*[\[\(\{]\s*at\s*[\]\)\}]\s*", "@", t, flags=re.I)
    t = re.sub(r"\s*[\[\(\{]\s*dot\s*[\]\)\}]\s*", ".", t, flags=re.I)
    return t


def visible_text(page: str) -> str:
    t = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", page)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", deobfuscate(t))


def emails_in(page: str, page_url: str) -> list[tuple[str, str]]:
    """(email, context) pairs from mailto links and visible text."""
    found: list[tuple[str, str]] = []
    for m in re.finditer(r'data-cfemail\s*=\s*["\']([0-9a-fA-F]+)["\']', page):
        e = cf_decode(m.group(1))
        if e and EMAIL_RE.fullmatch(e):
            found.append((e, visible_text(page[max(0, m.start() - 1200): m.end() + 400])))
    for m in re.finditer(r'href\s*=\s*["\']\s*mailto:([^"\'?]+)', page, re.I):
        for e in EMAIL_RE.findall(deobfuscate(m.group(1))):
            found.append((e, ""))
    text = visible_text(page)
    for m in EMAIL_RE.finditer(text):
        found.append((m.group(0), text[max(0, m.start() - 160): m.end() + 160]))
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for e, ctx in found:
        e = e.strip(".").lower()
        if e in seen or e.endswith(JUNK_EXT) or any(j in e for j in JUNK_DOMAINS):
            continue
        seen.add(e)
        out.append((e, ctx))
    return out


NOT_NAMES = {"customer", "resources", "contact", "home", "about", "services", "service", "privacy", "policy", "terms",
             "read", "more", "call", "now", "free", "estimate", "request", "quote", "our", "team", "email", "phone",
             "get", "learn", "view", "all", "rights", "reserved", "copyright", "follow", "click", "here", "send",
             "message", "office", "hours", "open", "closed", "monday", "tuesday", "wednesday", "thursday", "friday",
             "saturday", "sunday", "united", "states", "north", "south", "east", "west", "new", "main", "street",
             "road", "avenue", "suite", "llc", "inc", "the", "and", "for", "with", "your", "you", "we", "us"}


def person_name_near(ctx: str, title_match: re.Match | None) -> str | None:
    """A 'First Last' that sits within ~60 chars of a decision-maker title and isn't made of web boilerplate."""
    if not ctx:
        return None
    window = ctx if title_match is None else ctx[max(0, title_match.start() - 60): title_match.end() + 60]
    for nm in NAME_RE.finditer(window):
        parts = [p.strip(".").lower() for p in nm.group(1).split()]
        if not any(p in NOT_NAMES for p in parts):
            return nm.group(1)
    return None


def classify(email: str, ctx: str, site_host: str) -> tuple[str, str | None, int]:
    """-> (kind, owner_name, score). Higher score = better decision-maker candidate."""
    local, _, domain = email.partition("@")
    bare = local.split("+")[0].replace(".", "").replace("-", "").replace("_", "")
    generic = bare in GENERIC_LOCAL
    host = site_host.removeprefix("www.")
    same_domain = domain == host or host.endswith(domain)
    domain_label = domain.split(".")[0]
    # mandl@mandlplumbing.com is the company mailbox, not a person
    company_box = len(bare) >= 4 and (domain_label.startswith(bare) or bare in domain_label)
    score = 0
    if same_domain:
        score += 20
    elif domain in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com"):
        score += 5     # small businesses often run on a personal mailbox; still a real person
    if generic or company_box:
        return "generic", None, score + (5 if company_box else 0)
    score += 30
    title = TITLE_RE.search(ctx) if ctx else None
    if title:
        # a name is only recorded when it sits next to a decision-maker title; otherwise it's guesswork
        return "owner", person_name_near(ctx, title), score + 40
    return "personal", None, score


def contact_links(page: str, base: str) -> list[str]:
    links: list[str] = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\'#]+)', page, re.I):
        href = m.group(1).strip()
        low = href.lower()
        if any(w in low for w in CONTACT_LINK_WORDS):
            u = urljoin(base, href)
            if urlparse(u).netloc.lower() == urlparse(base).netloc.lower() and u not in links:
                links.append(u)
    return links[:MAX_EXTRA_PAGES]


def find_for(listing: dict[str, Any]) -> dict[str, Any]:
    site = Site(listing["website"])
    candidates: dict[str, dict[str, Any]] = {}
    pages = [site.start]
    home = site.fetch(site.start)
    if home is None and site.start.startswith("https://"):
        site.start = "http://" + site.start[len("https://"):]
        home = site.fetch(site.start)
    if home:
        pages += contact_links(home, site.start)
    visited = 0
    for i, url in enumerate(pages):
        page = home if i == 0 else site.fetch(url)
        if i:
            time.sleep(PAUSE_BETWEEN_PAGES)
        if not page:
            continue
        visited += 1
        for e, ctx in emails_in(page, url):
            kind, name, score = classify(e, ctx, site.host)
            cur = candidates.get(e)
            if cur is None or score > cur["score"]:
                candidates[e] = {"email": e, "kind": kind, "owner_name": name, "score": score, "source_url": url}
    ranked = sorted(candidates.values(), key=lambda c: -c["score"])
    best = ranked[0] if ranked else None
    return {
        "listing_id": listing["listing_id"],
        "email": best["email"] if best else None,
        "kind": best["kind"] if best else None,
        "owner_name": best["owner_name"] if best else None,
        "source_url": best["source_url"] if best else None,
        "candidates": [{k: c[k] for k in ("email", "kind", "owner_name", "source_url")} for c in ranked[:5]],
        "pages_read": visited,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget-minutes", type=int, default=300)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--dry-run", metavar="URL", help="inspect one website and print the result; nothing is written")
    a = ap.parse_args(argv)
    if a.dry_run:
        print(json.dumps(find_for({"listing_id": "00000000-0000-0000-0000-000000000000", "website": a.dry_run}), indent=2))
        return 0
    supa = worker.Supa()
    deadline = time.time() + a.budget_minutes * 60
    totals = {"looked_up": 0, "saved": 0, "owner": 0, "personal": 0, "generic": 0}
    while time.time() < deadline:
        batch = supa.rpc("nx_email_next", {"p_limit": a.batch}) or []
        if not batch:
            log("queue_empty")
            break
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            results = list(pool.map(lambda l: _safe(find_for, l), batch))
        ack = supa.rpc("nx_email_record", {"p_results": results})
        totals["looked_up"] += len(results)
        totals["saved"] += int((ack or {}).get("saved") or 0)
        for r in results:
            if r.get("kind"):
                totals[r["kind"]] += 1
        log("batch_done", size=len(results), **totals)
    log("finished", **totals)
    return 0


def _safe(fn, listing):  # noqa: ANN001
    try:
        return fn(listing)
    except Exception as exc:  # noqa: BLE001
        return {"listing_id": listing["listing_id"], "email": None, "kind": None, "owner_name": None,
                "source_url": None, "candidates": [], "error": str(exc)[:160]}


if __name__ == "__main__":
    raise SystemExit(main())
