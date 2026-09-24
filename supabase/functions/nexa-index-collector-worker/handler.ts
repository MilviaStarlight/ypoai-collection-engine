import {
  dedupeRows,
  normalizeNominatim,
  normalizeNpi,
  normalizePhoton,
  type ClaimedTarget,
  type NormalizedBusinessRow,
} from "../_shared/nexa_source_normalizers.ts";

export type CollectorControl = {
  collection_enabled: boolean;
  pilot_mode: boolean;
  batch_size: number;
  max_concurrency: number;
  allowed_states: string[];
  paused_source_families: string[];
};

export type CollectorTarget = ClaimedTarget & {
  source_family: string;
};

export type HeartbeatInput = {
  component_name: "collector";
  execution_id: string;
  status: "started" | "progress" | "completed" | "error";
  target_id: string;
  details: Record<string, unknown>;
};

export type RecordCollectionInput = {
  worker_name: string;
  target: CollectorTarget;
  source_name: string;
  source_url: string;
  rows: NormalizedBusinessRow[];
  payload: Record<string, unknown>;
};

export type CollectorDependencies = {
  authorize(request: Request): boolean;
  readControl(): Promise<CollectorControl>;
  claimTargets(input: {
    worker_name: string;
    execution_id: string;
    states: string[];
    limit: number;
  }): Promise<CollectorTarget[]>;
  writeHeartbeat(input: HeartbeatInput): Promise<void>;
  fetchJson(url: string): Promise<unknown>;
  recordCollection(input: RecordCollectionInput): Promise<{
    inserted_count: number;
    duplicate_count: number;
  }>;
  requeueTarget(input: {
    target_id: string;
    execution_id: string;
    reason: string;
  }): Promise<boolean>;
  now(): Date;
};

type CollectorRequest = {
  execution_id?: unknown;
  states?: unknown;
  batch_size?: unknown;
  dry_run?: unknown;
};

const NPI_TAXONOMY: Record<string, string> = {
  doctor: "Physician",
  dentist: "Dentist",
  "medical-services": "Clinic/Center",
  "urgent-care": "Clinic/Center",
  "physical-therapy": "Physical Therapist",
  chiropractors: "Chiropractor",
  pharmacies: "Pharmacy",
  optometrists: "Optometrist",
  veterinarians: "Veterinarian",
  "medical-clinics": "Clinic/Center",
  "mental-health-clinics": "Clinic/Center",
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function validStates(value: unknown, allowed: string[]): string[] {
  const requested = Array.isArray(value)
    ? value.map(String).map((state) => state.trim().toUpperCase())
    : [];
  const allowlist = new Set(allowed.map((state) => state.toUpperCase()));
  const filtered = requested.filter((state) => allowlist.has(state));
  return [...new Set(filtered.length ? filtered : [...allowlist])];
}

function queryUrl(target: CollectorTarget, endpoint: string): string {
  const query = `${target.category_slug.replaceAll("-", " ")} ${target.county} ${target.state} USA`;
  const params = new URLSearchParams({ q: query, limit: "10" });
  if (endpoint.includes("nominatim")) {
    params.set("format", "jsonv2");
    params.set("countrycodes", "us");
    params.set("addressdetails", "1");
    params.set("namedetails", "1");
    params.set("extratags", "1");
  } else {
    params.set("lang", "en");
  }
  return `${endpoint}?${params.toString()}`;
}

async function collectNpi(
  target: CollectorTarget,
  dependencies: CollectorDependencies,
): Promise<{ rows: NormalizedBusinessRow[]; sourceName: string; sourceUrl: string }> {
  const params = new URLSearchParams({
    version: "2.1",
    country_code: "US",
    address_purpose: "LOCATION",
    state: target.state,
    taxonomy_description: NPI_TAXONOMY[target.category_slug] ?? target.category_slug,
    limit: "10",
  });
  const sourceUrl = `https://npiregistry.cms.hhs.gov/api/?${params.toString()}`;
  const payload = asRecord(await dependencies.fetchJson(sourceUrl));
  const results = Array.isArray(payload.results) ? payload.results : [];
  const collectedAt = dependencies.now().toISOString();
  const rows = dedupeRows(
    results
      .map((item) => normalizeNpi(item, target, collectedAt))
      .filter((row): row is NormalizedBusinessRow => row !== null),
  );
  return { rows, sourceName: "NPPES NPI Registry API", sourceUrl };
}

async function collectOsm(
  target: CollectorTarget,
  dependencies: CollectorDependencies,
): Promise<{ rows: NormalizedBusinessRow[]; sourceName: string; sourceUrl: string }> {
  const photonUrl = queryUrl(target, "https://photon.komoot.io/api/");
  const photon = asRecord(await dependencies.fetchJson(photonUrl));
  const collectedAt = dependencies.now().toISOString();
  const photonRows = (Array.isArray(photon.features) ? photon.features : [])
    .map((item) => normalizePhoton(item, target, collectedAt))
    .filter((row): row is NormalizedBusinessRow => row !== null);
  if (photonRows.length) {
    return {
      rows: dedupeRows(photonRows).slice(0, 10),
      sourceName: "OpenStreetMap Photon",
      sourceUrl: photonUrl,
    };
  }

  const nominatimUrl = queryUrl(target, "https://nominatim.openstreetmap.org/search");
  const nominatim = await dependencies.fetchJson(nominatimUrl);
  const nominatimRows = (Array.isArray(nominatim) ? nominatim : [])
    .map((item) => normalizeNominatim(item, target, collectedAt))
    .filter((row): row is NormalizedBusinessRow => row !== null);
  return {
    rows: dedupeRows(nominatimRows).slice(0, 10),
    sourceName: "OpenStreetMap Nominatim",
    sourceUrl: nominatimUrl,
  };
}

async function collectTarget(target: CollectorTarget, dependencies: CollectorDependencies) {
  if (target.source_family === "npi_registry") return collectNpi(target, dependencies);
  if (target.source_family === "osm_fallback") return collectOsm(target, dependencies);
  return {
    rows: [] as NormalizedBusinessRow[],
    sourceName: "Unsupported source family",
    sourceUrl: "",
  };
}

export function createCollectorHandler(dependencies: CollectorDependencies) {
  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST") return json({ ok: false, error: "Method not allowed" }, 405);
    if (!dependencies.authorize(request)) return json({ ok: false, error: "Unauthorized" }, 401);

    const body = await request.json().catch(() => ({})) as CollectorRequest;
    const executionId = typeof body.execution_id === "string" ? body.execution_id.trim() : "";
    const requestedBatchSize = Number(body.batch_size ?? 1);
    const dryRun = body.dry_run === true;
    if (!executionId) return json({ ok: false, error: "execution_id is required" }, 400);
    if (!Number.isInteger(requestedBatchSize) || requestedBatchSize < 1 || requestedBatchSize > 10) {
      return json({ ok: false, error: "batch_size must be an integer from 1 to 10" }, 400);
    }

    const control = await dependencies.readControl();
    const base = {
      ok: true,
      execution_id: executionId,
      claimed: 0,
      inserted: 0,
      duplicates: 0,
      requeued: 0,
      quarantined: 0,
      results: [] as Array<Record<string, unknown>>,
    };
    if (!control.collection_enabled) {
      return json({ ...base, status: "collection_disabled" });
    }
    if (dryRun) {
      return json({ ...base, status: "dry_run_validated", states: validStates(body.states, control.allowed_states) });
    }

    const batchSize = Math.min(requestedBatchSize, control.batch_size, 10);
    const states = validStates(body.states, control.allowed_states);
    const targets = await dependencies.claimTargets({
      worker_name: "nexa-n8n-collector",
      execution_id: executionId,
      states,
      limit: batchSize,
    });
    base.claimed = targets.length;

    for (const target of targets) {
      await dependencies.writeHeartbeat({
        component_name: "collector",
        execution_id: executionId,
        status: "started",
        target_id: target.id,
        details: { source_family: target.source_family },
      });
      try {
        const collected = await collectTarget(target, dependencies);
        await dependencies.writeHeartbeat({
          component_name: "collector",
          execution_id: executionId,
          status: "progress",
          target_id: target.id,
          details: {
            source_family: target.source_family,
            rows_collected: collected.rows.length,
          },
        });
        if (!collected.rows.length || !collected.sourceUrl) {
          await dependencies.writeHeartbeat({
            component_name: "collector",
            execution_id: executionId,
            status: "error",
            target_id: target.id,
            details: { source_family: target.source_family, error_fingerprint: "no-source-backed-rows" },
          });
          const blocked = await dependencies.requeueTarget({
            target_id: target.id,
            execution_id: executionId,
            reason: "no-source-backed-rows",
          });
          if (blocked) base.quarantined += 1;
          else base.requeued += 1;
          base.results.push({
            target_id: target.id,
            status: blocked ? "insufficient_data" : "needs_private_review",
            source_family: target.source_family,
          });
          continue;
        }
        const recorded = await dependencies.recordCollection({
          worker_name: "nexa-n8n-collector",
          target,
          source_name: collected.sourceName,
          source_url: collected.sourceUrl,
          rows: collected.rows,
          payload: {
            execution_id: executionId,
            source_family: target.source_family,
            private_review_required: true,
          },
        });
        base.inserted += recorded.inserted_count;
        base.duplicates += recorded.duplicate_count;
        base.results.push({
          target_id: target.id,
          status: "completed",
          inserted: recorded.inserted_count,
          duplicates: recorded.duplicate_count,
        });
        await dependencies.writeHeartbeat({
          component_name: "collector",
          execution_id: executionId,
          status: "completed",
          target_id: target.id,
          details: {
            source_family: target.source_family,
            rows_collected: recorded.inserted_count,
            duplicate_count: recorded.duplicate_count,
          },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        await dependencies.writeHeartbeat({
          component_name: "collector",
          execution_id: executionId,
          status: "error",
          target_id: target.id,
          details: { source_family: target.source_family, error_fingerprint: "collector-error", message },
        });
        const blocked = await dependencies.requeueTarget({
          target_id: target.id,
          execution_id: executionId,
          reason: "collector-error",
        });
        if (blocked) base.quarantined += 1;
        else base.requeued += 1;
        base.results.push({ target_id: target.id, status: blocked ? "blocked" : "error", error: message });
      }
    }

    return json(base);
  };
}
