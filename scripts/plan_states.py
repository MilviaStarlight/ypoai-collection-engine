#!/usr/bin/env python3
"""Pick which states a collector run covers. Prints `states=[...]` for $GITHUB_OUTPUT.

Manual runs: the states typed in (or 'all' / 'launch'). Scheduled runs every 4 hours: one of six
rotating groups, so the collectors work around the clock and every state is covered once a day.
Group 0 is the 9 PostMyPrice launch states, Puerto Rico first.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

LAUNCH = "PR FL VA NY NJ MO MN OR WA".split()
REST = ("AL AK AZ AR CA CO CT DE DC GA HI ID IL IN IA KS KY LA ME MD MA MI MS MT NE NV NH NM NC ND "
        "OH OK PA RI SC SD TN TX UT VT WV WI WY").split()
ALL = LAUNCH + REST
GROUPS = 6


def groups() -> list[list[str]]:
    rest = [REST[i::GROUPS - 1] for i in range(GROUPS - 1)]
    return [LAUNCH] + rest


def plan(event: str, raw: str, hour: int) -> list[str]:
    if event == "schedule":
        return groups()[(hour // 4) % GROUPS]
    raw = (raw or "").strip().upper()
    if raw in ("", "ALL"):
        return ALL
    if raw == "LAUNCH":
        return LAUNCH
    return [s.strip() for s in raw.split(",") if s.strip() in ALL]


if __name__ == "__main__":
    hour = int(os.environ.get("PLAN_HOUR", datetime.now(timezone.utc).hour))
    print("states=" + json.dumps(plan(os.environ.get("EVENT", ""), os.environ.get("INPUT", ""), hour)))
    sys.exit(0)
