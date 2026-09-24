# YPOAI Collection Engine — operating rules for Codex / Claude windows

This repo runs the Yellow Pages of AI collection lanes. Workers are deterministic Python; they never call a model.
A Codex window that supervises a lane is a **supervisor, not a scraper**. Its whole job is one command and a few status checks.

## Supervisor rules (token budget ≈ 3–6k per window per day)
1. Start the worker with the one-liner in README.md. One worker per window. Never two.
2. Check status with `python scripts/worker.py status --lane Wn` at most every 2–4 hours. It prints six lines.
3. Never `cat`, `tail`, or read logs. Never ask a model to parse, classify, or fix a business record.
4. Never store files. The clone lives in `%TEMP%` and is deleted on exit. No CSVs, no notes on disk.
5. Secrets come only from the environment variable `SUPABASE_SERVICE_ROLE_KEY` (a Supabase secret key) or GitHub Actions secrets. Never paste a key into a file, a chat, or a commit.
6. Do not edit `collection_targets`, `import_rows`, or `businesses` by hand. Use the RPCs the worker uses.
7. Governance: Agenarys/Stellarys rules apply. Additive schema only; report any migration before applying; never invent results.

## Lanes
W1 Northeast · W2 Southeast · W3 South Central · W4 Midwest East · W5 Midwest West · W6 West · W7–W9 enrichment.
State lists live in Supabase (`nx_worker_lanes`), not in this repo. Zone order lives in `nx_zone_map`.
