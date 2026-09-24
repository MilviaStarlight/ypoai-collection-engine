import {
  createCollectorHandler,
  type CollectorControl,
  type CollectorTarget,
  type HeartbeatInput,
  type RecordCollectionInput,
} from "./handler.ts";
import { isAuthorizedEdgeRequest } from "./edge_auth.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-agenarys-workflow-key",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const supabaseUrl = requiredEnv("SUPABASE_URL").replace(/\/$/, "");
const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
const workflowKey = Deno.env.get("AGENARYS_INTERNAL_WORKFLOW_KEY")?.trim() ?? "";
const projectId = "ehzxnnxyliapvcvzzmyp";

function requiredEnv(name: string): string {
  const value = Deno.env.get(name)?.trim();
  if (!value) throw new Error(`${name} is missing`);
  return value;
}

function authorized(request: Request): boolean {
  return isAuthorizedEdgeRequest(request, serviceRoleKey, workflowKey, projectId);
}

function headers(profile?: string): Record<string, string> {
  const output: Record<string, string> = {
    apikey: serviceRoleKey,
    authorization: `Bearer ${serviceRoleKey}`,
    "content-type": "application/json",
  };
  if (profile) {
    output["accept-profile"] = profile;
    output["content-profile"] = profile;
  }
  return output;
}

async function responseJson(response: Response, context: string): Promise<unknown> {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = body && typeof body === "object" && "message" in body
      ? String((body as Record<string, unknown>).message)
      : `${context} failed with HTTP ${response.status}`;
    throw new Error(message);
  }
  return body;
}

async function readControl(): Promise<CollectorControl> {
  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/nexa_read_collector_control`, {
    method: "POST",
    headers: headers(),
    body: "{}",
  });
  const control = await responseJson(response, "Collector control lookup");
  if (!control || typeof control !== "object" || Array.isArray(control)) {
    throw new Error("Collector control row is missing");
  }
  return control as CollectorControl;
}

async function claimTargets(input: {
  worker_name: string;
  execution_id: string;
  states: string[];
  limit: number;
}): Promise<CollectorTarget[]> {
  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/nexa_claim_targets`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({
      p_worker_name: input.worker_name,
      p_execution_id: input.execution_id,
      p_states: input.states,
      p_limit: input.limit,
      p_lease_minutes: 15,
    }),
  });
  const body = await responseJson(response, "Target claim");
  return Array.isArray(body) ? body as CollectorTarget[] : [];
}

async function writeHeartbeat(input: HeartbeatInput): Promise<void> {
  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/nexa_write_heartbeat`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({
      p_component_name: input.component_name,
      p_execution_id: input.execution_id,
      p_status: input.status,
      p_target_id: input.target_id,
      p_details: input.details,
    }),
  });
  await responseJson(response, "Heartbeat write");
}

async function requeueTarget(input: {
  target_id: string;
  execution_id: string;
  reason: string;
}): Promise<boolean> {
  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/nexa_requeue_target`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({
      p_target_id: input.target_id,
      p_execution_id: input.execution_id,
      p_reason: input.reason,
    }),
  });
  const body = await responseJson(response, "Target requeue");
  return body === true;
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      "user-agent": "Agenarys-NEXA-Index-Collector/1.0",
    },
    signal: AbortSignal.timeout(30_000),
  });
  return responseJson(response, "Public source request");
}

async function recordCollection(input: RecordCollectionInput): Promise<{
  inserted_count: number;
  duplicate_count: number;
}> {
  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/agenarys_durable_record_collection`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({
      p_worker_name: input.worker_name,
      p_state: input.target.state,
      p_county: input.target.county,
      p_category_slug: input.target.category_slug,
      p_source_name: input.source_name,
      p_source_url: input.source_url,
      p_rows: input.rows,
      p_note: "NEXA free-first deterministic batch. Private review required before publication.",
      p_payload: input.payload,
    }),
  });
  const body = await responseJson(response, "Durable collection record");
  const row = Array.isArray(body) ? body[0] : body;
  const record = row && typeof row === "object" ? row as Record<string, unknown> : {};
  return {
    inserted_count: Number(record.inserted_count ?? 0),
    duplicate_count: Number(record.duplicate_count ?? 0),
  };
}

const handler = createCollectorHandler({
  authorize: authorized,
  readControl,
  claimTargets,
  writeHeartbeat,
  requeueTarget,
  fetchJson,
  recordCollection,
  now: () => new Date(),
});

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  const response = await handler(request);
  const responseHeaders = new Headers(response.headers);
  Object.entries(corsHeaders).forEach(([key, value]) => responseHeaders.set(key, value));
  return new Response(response.body, { status: response.status, headers: responseHeaders });
});
