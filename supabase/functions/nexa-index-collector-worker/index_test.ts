import assert from "node:assert/strict";
import test from "node:test";

import {
  createCollectorHandler,
  type CollectorDependencies,
  type CollectorControl,
} from "./handler.ts";
import type { ClaimedTarget } from "../_shared/nexa_source_normalizers.ts";


const target: ClaimedTarget & { source_family: string } = {
  id: "00000000-0000-0000-0000-000000000001",
  state: "FL",
  county: "Miami-Dade County",
  category_slug: "dentist",
  collected_count: 0,
  target_goal: 10,
  source_family: "npi_registry",
};

function dependencies(overrides: Partial<CollectorDependencies> = {}) {
  const calls = {
    claims: 0,
    heartbeats: [] as Array<Record<string, unknown>>,
    records: [] as Array<Record<string, unknown>>,
    requeues: [] as Array<Record<string, unknown>>,
    urls: [] as string[],
  };
  const control: CollectorControl = {
    collection_enabled: true,
    pilot_mode: true,
    batch_size: 1,
    max_concurrency: 1,
    allowed_states: ["FL"],
    paused_source_families: [],
  };
  const deps: CollectorDependencies = {
    authorize: () => true,
    readControl: async () => control,
    claimTargets: async () => {
      calls.claims += 1;
      return [target];
    },
    writeHeartbeat: async (heartbeat) => {
      calls.heartbeats.push(heartbeat);
    },
    fetchJson: async (url) => {
      calls.urls.push(url);
      return {
        results: [
          {
            number: "1104969039",
            basic: { organization_name: "183 DENTAL GROUP, PA" },
            addresses: [
              {
                address_purpose: "LOCATION",
                country_code: "US",
                state: "FL",
                city: "MIAMI",
              },
            ],
          },
        ],
      };
    },
    recordCollection: async (input) => {
      calls.records.push(input);
      return { inserted_count: 1, duplicate_count: 0 };
    },
    requeueTarget: async (input) => {
      calls.requeues.push(input);
      return false;
    },
    now: () => new Date("2026-07-13T18:00:00.000Z"),
    ...overrides,
  };
  return { calls, deps };
}

function request(body: unknown) {
  return new Request("https://example.test/functions/v1/nexa-index-collector-worker", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer test" },
    body: JSON.stringify(body),
  });
}

test("rejects an unauthorized request", async () => {
  const { deps } = dependencies({ authorize: () => false });
  const response = await createCollectorHandler(deps)(request({ execution_id: "x" }));
  assert.equal(response.status, 401);
});

test("rejects a batch larger than ten", async () => {
  const { deps } = dependencies();
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "x", states: ["FL"], batch_size: 11, dry_run: true }),
  );
  assert.equal(response.status, 400);
});

test("returns no work without claiming when collection is disabled", async () => {
  const { calls, deps } = dependencies({
    readControl: async () => ({
      collection_enabled: false,
      pilot_mode: true,
      batch_size: 1,
      max_concurrency: 1,
      allowed_states: ["FL"],
      paused_source_families: [],
    }),
  });
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "disabled", states: ["FL"], batch_size: 1 }),
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.claimed, 0);
  assert.equal(calls.claims, 0);
});

test("collects an NPI target and writes start progress and completion heartbeats", async () => {
  const { calls, deps } = dependencies();
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "pilot-1", states: ["FL"], batch_size: 1, dry_run: false }),
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.claimed, 1);
  assert.equal(body.inserted, 1);
  assert.equal(calls.records.length, 1);
  assert.deepEqual(calls.heartbeats.map((item) => item.status), ["started", "progress", "completed"]);
  assert.match(calls.urls[0], /npiregistry\.cms\.hhs\.gov/);
  assert.doesNotMatch(JSON.stringify(body), /gpt-4o-mini|claude/i);
});

test("dry run never claims or records rows", async () => {
  const { calls, deps } = dependencies();
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "pilot-dry", states: ["FL"], batch_size: 1, dry_run: true }),
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.claimed, 0);
  assert.equal(body.inserted, 0);
  assert.equal(calls.claims, 0);
  assert.equal(calls.records.length, 0);
});

test("falls back from Photon to Nominatim for an OSM target", async () => {
  const osmTarget = {
    ...target,
    category_slug: "cafe",
    source_family: "osm_fallback",
  };
  const { calls, deps } = dependencies({
    claimTargets: async () => [osmTarget],
    fetchJson: async (url) => {
      calls.urls.push(url);
      if (url.includes("photon.komoot.io")) return { features: [] };
      return [
        {
          osm_id: 999,
          osm_type: "node",
          name: "Fallback Cafe",
          address: { state: "Florida", county: "Miami-Dade County", city: "Miami" },
        },
      ];
    },
  });

  const response = await createCollectorHandler(deps)(
    request({ execution_id: "pilot-osm", states: ["FL"], batch_size: 1 }),
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.inserted, 1);
  assert.equal(calls.urls.length, 2);
  assert.match(calls.urls[0], /photon\.komoot\.io/);
  assert.match(calls.urls[1], /nominatim\.openstreetmap\.org/);
});

test("requeues and releases a target when no source-backed rows exist", async () => {
  const unsupported = { ...target, category_slug: "car-wash", source_family: "official_public_source" };
  const { calls, deps } = dependencies({ claimTargets: async () => [unsupported] });
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "pilot-empty", states: ["FL"], batch_size: 1 }),
  );
  const body = await response.json();
  assert.equal(body.requeued, 1);
  assert.equal(calls.requeues.length, 1);
  assert.deepEqual(calls.requeues[0], {
    target_id: target.id,
    execution_id: "pilot-empty",
    reason: "no-source-backed-rows",
  });
});

test("marks a repeated source-empty target insufficient instead of looping", async () => {
  const unsupported = { ...target, category_slug: "car-wash", source_family: "osm_fallback" };
  const { deps } = dependencies({
    claimTargets: async () => [unsupported],
    fetchJson: async () => ({ features: [] }),
    requeueTarget: async () => true,
  });
  const response = await createCollectorHandler(deps)(
    request({ execution_id: "pilot-empty-repeat", states: ["FL"], batch_size: 1 }),
  );
  const body = await response.json();
  assert.equal(body.requeued, 0);
  assert.equal(body.quarantined, 1);
  assert.equal(body.results[0].status, "insufficient_data");
});
