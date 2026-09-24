#!/usr/bin/env python3
"""
Minimal client for TypeSafe's Jev (System One model) via OpenRouter.

No new packages: uses `requests`, which this project already depends on.
Reads OPENROUTER_API_KEY from the environment only. Never prints the key.

Jev is not an LLM. You send a `state` (text) and typed `questions`
(choice / score / noul) and get back a typed answer with calibrated
probabilities and a confidence. It cannot generate prose.

Usage:
  python scripts/jev_client.py smoke            # one tiny live call, prints cost
  python scripts/jev_client.py classify-demo    # niche + intent demo on sample text

Reference:
  POST https://openrouter.ai/api/v1/systemone   (OpenRouter System One endpoint)
  model: typesafe/jev-1.13   (bare "jev-1.13" / "jev-latest" are remapped by OpenRouter)
  pricing (2026-09-21): $0.042 per 1M input tokens, output free, 32k context
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests

OPENROUTER_SYSTEMONE_URL = "https://openrouter.ai/api/v1/systemone"
DEFAULT_MODEL = "typesafe/jev-1.13"
TIMEOUT_SECONDS = 30

# Confidence below this goes to the next router tier or human review (blueprint section 6.5).
CONFIDENCE_FLOOR = 0.60


class JevError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not key:
        raise JevError("OPENROUTER_API_KEY is not set in the environment.")
    return key


def ask(
    state: str | dict[str, Any] | list[Any],
    questions: dict[str, dict[str, Any]],
    *,
    model: str = DEFAULT_MODEL,
    session_id: str | None = None,
) -> dict[str, Any]:
    """Send one System One request. Returns the parsed JSON response."""
    body: dict[str, Any] = {"model": model, "state": state, "questions": questions}
    if session_id:
        body["session_id"] = session_id[:256]
    response = requests.post(
        OPENROUTER_SYSTEMONE_URL,
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://yellowpagesofai.com",
            "X-Title": "YPOAI Model Router",
        },
        json=body,
        timeout=TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        # 402 = OpenRouter account has no credits; 429 = rate limit; 413 = state too large.
        raise JevError(f"HTTP {response.status_code}: {response.text[:300]}")
    return response.json()


def choice(instructions: str, criteria: dict[str, str]) -> dict[str, Any]:
    return {"type": "choice", "instructions": instructions, "criteria": criteria}


def score(instructions: str, levels: list[str]) -> dict[str, Any]:
    return {"type": "score", "instructions": instructions, "criteria": levels}


def noul(instructions: str) -> dict[str, Any]:
    return {"type": "noul", "instructions": instructions}


def gate(answer: dict[str, Any], floor: float = CONFIDENCE_FLOOR) -> tuple[Any, bool]:
    """Return (value, accepted). accepted=False means send to next tier / human review."""
    kind = answer.get("type")
    if kind == "choice":
        return answer.get("choice"), float(answer.get("confidence", 0)) >= floor
    if kind == "score":
        return answer.get("score"), float(answer.get("confidence", 0)) >= floor
    if kind == "noul":
        p = float(answer.get("noul", 0.5))
        return p >= 0.5, abs(p - 0.5) >= (floor - 0.5)
    return None, False


# --- demos -------------------------------------------------------------------

INBOUND_INTENT = choice(
    "What does the sender of this reply want? The reply is to an email telling a business "
    "it was listed for free in the Yellow Pages of AI directory.",
    {
        "remove": "Wants the listing removed or to stop receiving emails (English or Spanish).",
        "correct_info": "Says some listed detail is wrong or wants to update information.",
        "interested": "Asks about membership, pricing, catalog, or wants to buy or talk.",
        "question": "Asks a question that needs a written answer.",
        "other": "Auto-reply, bounce, spam, or unrelated.",
    },
)
INBOUND_SPANISH = noul("The message is written mainly in Spanish.")
INBOUND_URGENCY = score("How urgent is the sender's tone?", ["Calm", "Wants a prompt reply", "Angry or threatening"])

NICHE_GROUP = choice(
    "Which directory group best fits this business, based on its name, category tags, and website text?",
    {
        "home_services": "Trades and home repair: handyman, plumbing, roofing, fencing, concrete, cleaning, landscaping.",
        "automotive": "Auto body, repair, detailing, tires, motorcycle, boat, RV, glass.",
        "professional": "Bookkeeping, notary, financial, real estate, architect, engineer, marketing, IT, legal, insurance.",
        "food": "Restaurant, cafe, bakery, food truck, catering, juice bar.",
        "retail": "Stores selling goods: clothing, furniture, jewelry, grocery, electronics, florist, books.",
        "fitness_wellness": "Yoga, pilates, boxing, crossfit, personal training, massage.",
        "beauty": "Hair, nails, lashes, med spa, spa, tattoo.",
        "pet": "Grooming, training, sitting, boarding, daycare, walking, veterinary.",
        "medical": "Doctors, dentists, clinics, urgent care, physical therapy.",
        "events_creative": "Event planning, wedding services, photography, home organizing, laundry.",
    },
)
IS_LOCAL_BUSINESS = noul(
    "This is a single local business location a consumer could contact directly, "
    "not a directory, aggregator, franchise headquarters, government office, or parked domain."
)


def _print_answers(result: dict[str, Any]) -> None:
    for name, answer in result.get("answers", {}).items():
        value, accepted = gate(answer)
        conf = answer.get("confidence", answer.get("noul"))
        print(f"  {name:16s} -> {value!s:14s} confidence={conf}  {'OK' if accepted else 'REVIEW'}")
    usage = result.get("usage", {})
    print(f"  usage: in={usage.get('input_tokens')} out={usage.get('output_tokens')} cost_usd={usage.get('cost')}")


def main(argv: list[str]) -> int:
    cmd = argv[1] if len(argv) > 1 else "smoke"
    if cmd == "smoke":
        result = ask("Please remove my business from your list. Gracias.", {"intent": INBOUND_INTENT, "spanish": INBOUND_SPANISH})
        print(f"model={result.get('model')} provider={result.get('provider')}")
        _print_answers(result)
        return 0
    if cmd == "classify-demo":
        sample = (
            "Name: Riverbend Fence & Deck LLC. Tags: craft=fence, craft=carpenter. "
            "Website text: Family-owned since 2009. Cedar and vinyl fencing, custom decks, free estimates in Boone County MO."
        )
        result = ask(sample, {"group": NICHE_GROUP, "is_local": IS_LOCAL_BUSINESS})
        _print_answers(result)
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except JevError as exc:
        print(f"JEV ERROR: {exc}")
        sys.exit(1)
