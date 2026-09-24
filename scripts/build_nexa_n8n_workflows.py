"""Generate importable, secret-free n8n workflows for the NEXA collector."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "n8n"
ERROR_WORKFLOW_ID = "N5FeJODzdzqcWSjp"
OPENROUTER_CREDENTIAL = {
    "openRouterApi": {
        "id": "GTUM1pDMSZExZhai",
        "name": "AGENARYS-Kimi-Engineers",
    }
}
PAID_REPAIR_MODEL = "deepseek/deepseek-v4-flash"
N8N_API_CREDENTIAL = {
    "httpHeaderAuth": {
        "id": "LwXXgDsfN8UTFmen",
        "name": "AGENARYS-n8n-API",
    }
}
SUPABASE_CREDENTIAL = {
    "httpCustomAuth": {
        "id": "hMelugR5uE5dml2s",
        "name": "AGENARYS-NEXA-Supabase-ServiceRole",
    }
}
SUPABASE_BASE = "https://ehzxnnxyliapvcvzzmyp.supabase.co"
N8N_BASE = "http://<agenarys-executor>"


def node(
    name: str,
    node_type: str,
    position: tuple[int, int],
    parameters: dict[str, Any],
    *,
    type_version: float = 1,
    credentials: dict[str, Any] | None = None,
    on_error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "parameters": parameters,
        "id": name.lower().replace(" ", "-")[:64],
        "name": name,
        "type": node_type,
        "typeVersion": type_version,
        "position": list(position),
    }
    if credentials:
        result["credentials"] = credentials
    if on_error:
        result["onError"] = on_error
    return result


def link(target: str, output_index: int = 0) -> dict[str, Any]:
    return {"node": target, "type": "main", "index": output_index}


def http_headers(*pairs: tuple[str, str]) -> dict[str, Any]:
    return {
        "parameters": [
            {"name": name, "value": value}
            for name, value in pairs
        ]
    }


def openrouter_body(purpose: str) -> str:
    return "=" + json.dumps(
        {
            "model": "qwen/qwen3-coder:free",
            "models": ["openai/gpt-oss-20b:free"],
            "response_format": {"type": "json_object"},
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Return JSON only. Advisory analysis only. Never propose database, "
                        "credential, workflow-definition, deletion, paid-model, or outreach changes."
                    ),
                },
                {"role": "user", "content": purpose},
            ],
        },
        separators=(",", ":"),
    )


def paid_repair_body() -> str:
    return "={{ JSON.stringify($json.paid_repair_request) }}"


def collector_workflow() -> dict[str, Any]:
    validate_wake = """
return [{ json: $json.body || {} }];
""".strip()
    build_request = """
const incoming = $json.body || $json || {};
const states = Array.isArray(incoming.states)
  ? incoming.states
  : [];
const configured = 1;
const requested = Number(incoming.batch_size || configured);
return [{ json: {
  execution_id: `n8n-${$execution.id}`,
  states: states.map(s => String(s).trim().toUpperCase()).filter(Boolean),
  batch_size: Math.max(1, Math.min(requested, configured, 10)),
  dry_run: incoming.dry_run === true,
} }];
""".strip()
    validate_advisory = """
const worker = $('Call Collector Worker').first().json;
const content = $json.message?.content || $json.choices?.[0]?.message?.content || '{}';
let advisory = { valid: false, reason: 'No valid advisory JSON' };
try { advisory = { valid: true, ...JSON.parse(content) }; } catch (_) {}
return [{ json: { ...worker, advisory, advisory_only: true } }];
""".strip()

    nodes = [
        node("Schedule Every 15 Minutes", "n8n-nodes-base.scheduleTrigger", (-900, -120), {
            "rule": {"interval": [{"field": "minutes", "minutesInterval": 15}]}
        }, type_version=1.2),
        node("Manual Pilot Trigger", "n8n-nodes-base.manualTrigger", (-900, 40), {}, type_version=1),
        node("Authenticated Wake Webhook", "n8n-nodes-base.webhook", (-900, 220), {
            "httpMethod": "POST",
            "path": "agenarys-nexa-index-collector-wake-v1",
            "authentication": "headerAuth",
            "responseMode": "onReceived",
            "options": {},
        }, type_version=2, credentials=N8N_API_CREDENTIAL),
        node("Validate Collector Wake", "n8n-nodes-base.code", (-660, 220), {"jsCode": validate_wake}, type_version=2),
        node("Build Collector Request", "n8n-nodes-base.code", (-420, 20), {"jsCode": build_request}, type_version=2),
        node("Call Collector Worker", "n8n-nodes-base.httpRequest", (-140, 20), {
            "method": "POST",
            "url": f"{SUPABASE_BASE}/functions/v1/nexa-index-collector-worker",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpCustomAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "={{ JSON.stringify($json) }}",
            "options": {"timeout": 120000},
        }, type_version=4.2, credentials=SUPABASE_CREDENTIAL),
        node("Any Ambiguous Records?", "n8n-nodes-base.if", (120, 20), {
            "conditions": {
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict"},
                "conditions": [{
                    "id": "ambiguous-records",
                    "leftValue": "={{ Array.isArray($json.results) && $json.results.some(r => r.status === 'needs_private_review') }}",
                    "rightValue": True,
                    "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                }],
                "combinator": "and",
            },
            "options": {},
        }, type_version=2.2),
        node("Free-First Advisory Classifier", "n8n-nodes-base.httpRequest", (380, -80), {
            "method": "POST",
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "authentication": "predefinedCredentialType",
            "nodeCredentialType": "openRouterApi",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": openrouter_body("Classify only the source-backed ambiguous records from the prior node for private review."),
            "options": {"timeout": 60000},
        }, type_version=4.2, credentials=OPENROUTER_CREDENTIAL, on_error="continueRegularOutput"),
        node("Validate Advisory JSON", "n8n-nodes-base.code", (640, -80), {"jsCode": validate_advisory}, type_version=2),
        node("Record Collector Result", "n8n-nodes-base.code", (900, 20), {
            "jsCode": "return [{ json: { ...$json, workflow_status: 'collector_cycle_finished', finished_at: new Date().toISOString() } }];"
        }, type_version=2),
    ]
    connections = {
        "Schedule Every 15 Minutes": {"main": [[link("Build Collector Request")]]},
        "Manual Pilot Trigger": {"main": [[link("Build Collector Request")]]},
        "Authenticated Wake Webhook": {"main": [[link("Validate Collector Wake")]]},
        "Validate Collector Wake": {"main": [[link("Build Collector Request")]]},
        "Build Collector Request": {"main": [[link("Call Collector Worker")]]},
        "Call Collector Worker": {"main": [[link("Any Ambiguous Records?")]]},
        "Any Ambiguous Records?": {
            "main": [[link("Free-First Advisory Classifier")], [link("Record Collector Result")]]
        },
        "Free-First Advisory Classifier": {"main": [[link("Validate Advisory JSON")]]},
        "Validate Advisory JSON": {"main": [[link("Record Collector Result")]]},
    }
    return {
        "name": "AGENARYS-NEXA-INDEX-COLLECTOR-FREEFIRST-V1",
        "nodes": nodes,
        "connections": connections,
        "settings": {
            "executionOrder": "v1",
            "saveManualExecutions": True,
            "callerPolicy": "workflowsFromSameOwner",
            "errorWorkflow": ERROR_WORKFLOW_ID,
        },
        "active": False,
        "tags": [],
    }


def guardian_workflow() -> dict[str, Any]:
    validate_wake = """
return [{ json: $json.body || {} }];
""".strip()
    guardian_request = """
return [{ json: {
  p_component_name: 'guardian',
  p_execution_id: `guardian-${$execution.id}`,
  p_status: 'progress',
  p_target_id: null,
  p_details: { checked_at: new Date().toISOString() }
} }];
""".strip()
    policy = """
const snapshot = Array.isArray($json) ? ($json[0] || {}) : $json;
const now = Date.now();
const minutes = value => value ? (now - new Date(value).getTime()) / 60000 : 9999;
let action = 'NO_ACTION';
let incident_type = 'HEALTHY';
let reason = 'Collector is progressing';
if (Number(snapshot.ready_count || 0) <= 0) {
  reason = 'No eligible work';
} else if (Number(snapshot.repair_attempts || 0) >= 2) {
  action = 'QUARANTINE'; incident_type = 'REPAIR_LIMIT'; reason = 'Two repairs exhausted';
} else if (Number(snapshot.repeated_target_count || 0) >= 3 || Number(snapshot.repeated_error_count || 0) >= 3) {
  action = 'STOP_AND_REQUEUE'; incident_type = 'LOOP'; reason = 'Repeated target or error';
} else if (snapshot.lease_expired === true && minutes(snapshot.last_progress_at) >= 15) {
  action = 'RELEASE_STALE_LEASE'; incident_type = 'STALE_LEASE'; reason = 'Expired lease without progress';
} else if (snapshot.error_fingerprint === 'model-rate-limit') {
  action = 'REQUEUE_FREE_MODEL'; incident_type = 'FREE_MODEL_UNAVAILABLE'; reason = 'No paid fallback';
} else if (!snapshot.last_collector_heartbeat || minutes(snapshot.last_collector_heartbeat) >= 20) {
  action = 'WAKE_COLLECTOR'; incident_type = 'SLEEPING'; reason = 'Collector heartbeat is stale';
}
const allowed = ['NO_ACTION','WAKE_COLLECTOR','RELEASE_STALE_LEASE','STOP_AND_REQUEUE','REQUEUE_FREE_MODEL','QUARANTINE'];
if (!allowed.includes(action)) throw new Error(`Forbidden guardian action: ${action}`);
return [{ json: { ...snapshot, action, incident_type, reason, actionable: action !== 'NO_ACTION', guardian_execution_id: `guardian-${$execution.id}` } }];
""".strip()
    preserve_action = """
const decision = $('Deterministic Guardian Policy').first().json;
const content = $json.message?.content || $json.choices?.[0]?.message?.content || '{}';
let advisory = { valid: false };
try { advisory = { valid: true, ...JSON.parse(content) }; } catch (_) {}
const result = { ...decision, advisory, action: decision.action, advisory_only: true };
result.wake_payload = { reason: result.reason, source: 'guardian' };
result.release_payload = {
  p_target_id: result.target_id || null,
  p_guardian_execution_id: result.guardian_execution_id,
  p_expected_execution_id: result.collector_execution_id || null
};
result.incident_payload = {
  p_incident_key: [result.incident_type, result.target_id || 'none', result.error_fingerprint || 'none'].join(':'),
  p_incident_type: result.incident_type,
  p_target_id: result.target_id || null,
  p_collector_execution_id: result.collector_execution_id || null,
  p_guardian_execution_id: result.guardian_execution_id,
  p_repair_action: result.action,
  p_evidence: { reason: result.reason, advisory: result.advisory || null }
};
return [{ json: result }];
""".strip()
    paid_gate = """
const current = $json;
const paidActions = new Set(['STOP_AND_REQUEUE', 'REQUEUE_FREE_MODEL', 'QUARANTINE']);
const paidIncidentTypes = new Set(['LOOP', 'REPAIR_LIMIT', 'FREE_MODEL_UNAVAILABLE']);
const repeatedErrors = Number(current.repeated_error_count || 0);
const needsPaidRepair = paidActions.has(current.action)
  || paidIncidentTypes.has(current.incident_type)
  || repeatedErrors >= 3;
const paidRepairRequest = needsPaidRepair ? {
  model: 'deepseek/deepseek-v4-flash',
  response_format: { type: 'json_object' },
  temperature: 0,
  max_tokens: 700,
  messages: [
    {
      role: 'system',
      content: 'Return JSON only. You are the paid Layer 3 repair supervisor for a low-budget launch system. Spend tokens carefully. Diagnose the smallest reversible repair, never reveal or request secrets, never approve publishing, and never change the deterministic guardian action.'
    },
    {
      role: 'user',
      content: 'Guardian red flag context: ' + JSON.stringify(current) + '\\nReturn {"severity","likely_cause","recommended_repair","cost_note","safe_to_retry"}. Keep recommended_repair operational and reversible.'
    }
  ]
} : null;
return [{ json: {
  ...current,
  paid_repair_needed: needsPaidRepair,
  paid_repair_model: needsPaidRepair ? 'deepseek/deepseek-v4-flash' : null,
  paid_repair_request: paidRepairRequest,
  paid_repair_reason: needsPaidRepair
    ? 'Red-flag guardian action requires paid repair supervisor'
    : 'Free guardian action is sufficient'
} }];
""".strip()
    attach_paid_repair = """
const base = $('Paid Escalation Needed?').first().json;
const content = $json.message?.content || $json.choices?.[0]?.message?.content || '{}';
let paid_repair = { valid: false, reason: 'No valid paid repair JSON' };
try { paid_repair = { valid: true, ...JSON.parse(content) }; } catch (_) {}
const result = { ...base, paid_repair, paid_repair_used: true };
result.incident_payload = {
  ...base.incident_payload,
  p_evidence: {
    ...(base.incident_payload?.p_evidence || {}),
    paid_repair_model: base.paid_repair_model,
    paid_repair
  }
};
return [{ json: result }];
""".strip()
    build_incident_record_payload = """
let source;
try {
  source = $('Attach Paid Repair Advice').first().json;
} catch (_) {
  source = $('Mark Paid Escalation Need').first().json;
}
return [{ json: source.incident_payload }];
""".strip()

    nodes = [
        node("Schedule Every 5 Minutes", "n8n-nodes-base.scheduleTrigger", (-980, -100), {
            "rule": {"interval": [{"field": "minutes", "minutesInterval": 5}]}
        }, type_version=1.2),
        node("Authenticated Guardian Wake Webhook", "n8n-nodes-base.webhook", (-980, 100), {
            "httpMethod": "POST",
            "path": "agenarys-nexa-index-guardian-wake-v1",
            "authentication": "headerAuth",
            "responseMode": "onReceived",
            "options": {},
        }, type_version=2, credentials=N8N_API_CREDENTIAL),
        node("Validate Guardian Wake", "n8n-nodes-base.code", (-760, 100), {"jsCode": validate_wake}, type_version=2),
        node("Build Guardian Request", "n8n-nodes-base.code", (-540, -20), {"jsCode": guardian_request}, type_version=2),
        node("Write Guardian Heartbeat", "n8n-nodes-base.httpRequest", (-300, -20), {
            "method": "POST",
            "url": f"{SUPABASE_BASE}/rest/v1/rpc/nexa_write_heartbeat",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpCustomAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "={{ JSON.stringify($json) }}",
            "options": {"response": {"response": {"responseFormat": "text"}}},
        }, type_version=4.2, credentials=SUPABASE_CREDENTIAL),
        node("Read Guardian Snapshot", "n8n-nodes-base.httpRequest", (-60, -20), {
            "method": "POST",
            "url": f"{SUPABASE_BASE}/rest/v1/rpc/nexa_guardian_snapshot",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpCustomAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "{}",
            "options": {},
        }, type_version=4.2, credentials=SUPABASE_CREDENTIAL),
        node("Deterministic Guardian Policy", "n8n-nodes-base.code", (180, -20), {"jsCode": policy}, type_version=2),
        node("Action Needed?", "n8n-nodes-base.if", (420, -20), {
            "conditions": {
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict"},
                "conditions": [{
                    "id": "actionable",
                    "leftValue": "={{ $json.actionable }}",
                    "rightValue": True,
                    "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                }],
                "combinator": "and",
            },
            "options": {},
        }, type_version=2.2),
        node("Free-First Guardian Diagnostic", "n8n-nodes-base.httpRequest", (660, -120), {
            "method": "POST",
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "authentication": "predefinedCredentialType",
            "nodeCredentialType": "openRouterApi",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": openrouter_body("Explain the deterministic guardian incident. Do not change or replace the selected action."),
            "options": {"timeout": 60000},
        }, type_version=4.2, credentials=OPENROUTER_CREDENTIAL, on_error="continueRegularOutput"),
        node("Preserve Deterministic Action", "n8n-nodes-base.code", (900, -120), {"jsCode": preserve_action}, type_version=2),
        node("Mark Paid Escalation Need", "n8n-nodes-base.code", (1140, -120), {"jsCode": paid_gate}, type_version=2),
        node("Paid Escalation Needed?", "n8n-nodes-base.if", (1380, -120), {
            "conditions": {
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict"},
                "conditions": [{
                    "id": "paid-red-flag",
                    "leftValue": "={{ $json.paid_repair_needed === true }}",
                    "rightValue": True,
                    "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                }],
                "combinator": "and",
            },
            "options": {},
        }, type_version=2.2),
        node("Paid Layer 3 Repair Supervisor", "n8n-nodes-base.httpRequest", (1620, -260), {
            "method": "POST",
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "authentication": "predefinedCredentialType",
            "nodeCredentialType": "openRouterApi",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": paid_repair_body(),
            "options": {"timeout": 60000},
        }, type_version=4.2, credentials=OPENROUTER_CREDENTIAL, on_error="continueRegularOutput"),
        node("Attach Paid Repair Advice", "n8n-nodes-base.code", (1860, -260), {"jsCode": attach_paid_repair}, type_version=2),
        node("Wake Collector?", "n8n-nodes-base.if", (2100, -120), {
            "conditions": {"options": {"typeValidation": "strict"}, "conditions": [{
                "id": "wake", "leftValue": "={{ $json.action }}", "rightValue": "WAKE_COLLECTOR",
                "operator": {"type": "string", "operation": "equals"},
            }], "combinator": "and"}, "options": {},
        }, type_version=2.2),
        node("Wake Collector", "n8n-nodes-base.httpRequest", (2340, -240), {
            "method": "POST",
            "url": f"{N8N_BASE}/webhook/agenarys-nexa-index-collector-wake-v1",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpHeaderAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "={{ JSON.stringify($json.wake_payload) }}",
            "options": {},
        }, type_version=4.2, credentials=N8N_API_CREDENTIAL),
        node("Release Stale Lease?", "n8n-nodes-base.if", (2340, 0), {
            "conditions": {"options": {"typeValidation": "strict"}, "conditions": [{
                "id": "release", "leftValue": "={{ $json.action }}", "rightValue": "RELEASE_STALE_LEASE",
                "operator": {"type": "string", "operation": "equals"},
            }], "combinator": "and"}, "options": {},
        }, type_version=2.2),
        node("Release Stale Lease", "n8n-nodes-base.httpRequest", (2580, -40), {
            "method": "POST",
            "url": f"{SUPABASE_BASE}/rest/v1/rpc/nexa_release_stale_lease",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpCustomAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "={{ JSON.stringify($json.release_payload) }}",
            "options": {"response": {"response": {"responseFormat": "text"}}},
        }, type_version=4.2, credentials=SUPABASE_CREDENTIAL),
        node("Stop Looping Execution?", "n8n-nodes-base.if", (2580, 120), {
            "conditions": {"options": {"typeValidation": "strict"}, "conditions": [{
                "id": "stop", "leftValue": "={{ $json.action }}", "rightValue": "STOP_AND_REQUEUE",
                "operator": {"type": "string", "operation": "equals"},
            }], "combinator": "and"}, "options": {},
        }, type_version=2.2),
        node("Stop Looping Execution", "n8n-nodes-base.httpRequest", (2820, 80), {
            "method": "DELETE",
            "url": "={{ '" + N8N_BASE + "/api/v1/executions/' + String($json.collector_execution_id || '').replace(/^n8n-/, '') }}",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpHeaderAuth",
            "options": {},
        }, type_version=4.2, credentials=N8N_API_CREDENTIAL, on_error="continueRegularOutput"),
        node("Build Incident Record Payload", "n8n-nodes-base.code", (3060, -80), {"jsCode": build_incident_record_payload}, type_version=2),
        node("Record Guardian Incident", "n8n-nodes-base.httpRequest", (3300, -80), {
            "method": "POST",
            "url": f"{SUPABASE_BASE}/rest/v1/rpc/nexa_record_incident",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpCustomAuth",
            "sendBody": True,
            "contentType": "json",
            "specifyBody": "json",
            "jsonBody": "={{ JSON.stringify($json) }}",
            "options": {},
        }, type_version=4.2, credentials=SUPABASE_CREDENTIAL),
        node("Finalize Guardian Cycle", "n8n-nodes-base.code", (3540, 40), {
            "jsCode": "return [{ json: { ...$json, guardian_cycle_finished: true, finished_at: new Date().toISOString() } }];"
        }, type_version=2),
    ]

    connections = {
        "Schedule Every 5 Minutes": {"main": [[link("Build Guardian Request")]]},
        "Authenticated Guardian Wake Webhook": {"main": [[link("Validate Guardian Wake")]]},
        "Validate Guardian Wake": {"main": [[link("Build Guardian Request")]]},
        "Build Guardian Request": {"main": [[link("Write Guardian Heartbeat")]]},
        "Write Guardian Heartbeat": {"main": [[link("Read Guardian Snapshot")]]},
        "Read Guardian Snapshot": {"main": [[link("Deterministic Guardian Policy")]]},
        "Deterministic Guardian Policy": {"main": [[link("Action Needed?")]]},
        "Action Needed?": {"main": [[link("Free-First Guardian Diagnostic")], [link("Finalize Guardian Cycle")]]},
        "Free-First Guardian Diagnostic": {"main": [[link("Preserve Deterministic Action")]]},
        "Preserve Deterministic Action": {"main": [[link("Mark Paid Escalation Need")]]},
        "Mark Paid Escalation Need": {"main": [[link("Paid Escalation Needed?")]]},
        "Paid Escalation Needed?": {"main": [[link("Paid Layer 3 Repair Supervisor")], [link("Wake Collector?")]]},
        "Paid Layer 3 Repair Supervisor": {"main": [[link("Attach Paid Repair Advice")]]},
        "Attach Paid Repair Advice": {"main": [[link("Wake Collector?")]]},
        "Wake Collector?": {"main": [[link("Wake Collector")], [link("Release Stale Lease?")]]},
        "Wake Collector": {"main": [[link("Build Incident Record Payload")]]},
        "Release Stale Lease?": {"main": [[link("Release Stale Lease")], [link("Stop Looping Execution?")]]},
        "Release Stale Lease": {"main": [[link("Build Incident Record Payload")]]},
        "Stop Looping Execution?": {"main": [[link("Stop Looping Execution")], [link("Build Incident Record Payload")]]},
        "Stop Looping Execution": {"main": [[link("Build Incident Record Payload")]]},
        "Build Incident Record Payload": {"main": [[link("Record Guardian Incident")]]},
        "Record Guardian Incident": {"main": [[link("Finalize Guardian Cycle")]]},
    }
    return {
        "name": "AGENARYS-NEXA-INDEX-COLLECTOR-GUARDIAN-V1",
        "nodes": nodes,
        "connections": connections,
        "settings": {
            "executionOrder": "v1",
            "saveManualExecutions": True,
            "callerPolicy": "workflowsFromSameOwner",
            "errorWorkflow": ERROR_WORKFLOW_ID,
        },
        "active": False,
        "tags": [],
    }


def write_workflow(filename: str, workflow: dict[str, Any]) -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    path = OUTPUT / filename
    path.write_text(json.dumps(workflow, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(path.name)


def main() -> int:
    write_workflow("AGENARYS-NEXA-INDEX-COLLECTOR-FREEFIRST-V1.json", collector_workflow())
    write_workflow("AGENARYS-NEXA-INDEX-COLLECTOR-GUARDIAN-V1.json", guardian_workflow())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
