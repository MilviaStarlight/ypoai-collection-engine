# YPOAI Collection Engine

Deterministic, token-free collection lanes for the Yellow Pages of AI directory (Agenarys / Stellarys Resonance LLC).
Code only. All state lives in Supabase; no data, secrets, or logs live in this repo or on any PC.

## Run a lane from any PC (ephemeral clone, nothing left behind)

PowerShell, with `SUPABASE_SERVICE_ROLE_KEY` set as a user environment variable:

```powershell
$env:YPOAI_LANE="W4"; $t="$env:TEMP\ypoai-$env:YPOAI_LANE"; if (Test-Path $t) { Remove-Item -Recurse -Force $t }; git clone --depth 1 https://github.com/MilviaStarlight/ypoai-collection-engine $t; pip install -q -r "$t\requirements.txt"; python "$t\scripts\worker.py" start --lane $env:YPOAI_LANE --hours 8; Remove-Item -Recurse -Force $t
```

Simplest way to keep a PC lane running day and night (no Codex or Claude window needed): download `scripts/run_lane.cmd`
once, then in its own window run `run_lane.cmd W3`. It re-clones and restarts every 8 hours until the window is closed.
Two windows per PC: PC-A W1+W2, PC-B W3+W4, PC-C W5+W6.

Status (six lines, safe to run any time):

```powershell
python scripts\worker.py status --lane W4
```

## What the worker does
1. Reads its lane (states, batch size) from Supabase `nx_worker_lanes`.
2. Claims county × category cells through the row-locked RPC `agenarys_durable_next_targets` — two workers can never take the same cell.
3. Pulls every tagged business for that county and category from OpenStreetMap via the Overpass API in one request (fair-use paced).
4. Normalizes rows with exact state and county attribution, marks them `private_review_required`.
5. Records through `agenarys_durable_record_collection` (dedupes, updates the target), closes the cell via `nx_close_cell`.
6. Heartbeats to `nexa_collector_heartbeats` as `pc-worker` so the n8n guardian and `nx_lane_status` can see it.

Cells whose category has no free deterministic source are parked as `blocked` with a reason; the licensing-board lane reopens them.

## Layout
- `scripts/worker.py` — the lane worker (start / status / dry-run)
- `scripts/jev_client.py` — TypeSafe Jev decision client (classification tier, via OpenRouter)
- `scripts/continuous_collector.py`, `adapters/` — earlier Team B collector and NPI adapter (reference)
- `supabase/functions/` — Edge Functions source (collector worker, normalizers)
- `sql/` — additive migrations (lanes, zone map, niche identities)

- Blueprints and to-do lists are kept privately outside this repo
- `tests/` — `python -m unittest discover -s tests`

## Rules
See `AGENTS.md`. Additive schema only. Never invent results. Secrets only in environment variables or GitHub Actions secrets.
