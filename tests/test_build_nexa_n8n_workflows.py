import json
from pathlib import Path
import re
import unittest


N8N_DIR = Path("n8n")
COLLECTOR = "AGENARYS-NEXA-INDEX-COLLECTOR-FREEFIRST-V1.json"
GUARDIAN = "AGENARYS-NEXA-INDEX-COLLECTOR-GUARDIAN-V1.json"


class WorkflowBuildTests(unittest.TestCase):
    def load(self, name):
        return json.loads((N8N_DIR / name).read_text(encoding="utf-8"))

    def test_collector_is_free_only_and_has_error_workflow(self):
        workflow = self.load(COLLECTOR)
        text = json.dumps(workflow)
        self.assertIn("qwen/qwen3-coder:free", text)
        self.assertIn("openai/gpt-oss-20b:free", text)
        self.assertNotIn("gpt-4o-mini", text)
        self.assertEqual(workflow["settings"]["errorWorkflow"], "N5FeJODzdzqcWSjp")

    def test_collector_has_three_triggers_and_bounded_worker_call(self):
        workflow = self.load(COLLECTOR)
        names = {node["name"] for node in workflow["nodes"]}
        self.assertTrue(
            {
                "Schedule Every 15 Minutes",
                "Manual Pilot Trigger",
                "Authenticated Wake Webhook",
                "Build Collector Request",
                "Call Collector Worker",
            }.issubset(names)
        )
        text = json.dumps(workflow)
        self.assertIn("nexa-index-collector-worker", text)
        self.assertIn("Math.min(requested, configured, 10)", text)
        self.assertNotIn("$env", text)
        self.assertNotIn("states: ['FL']", text)

    def test_guardian_has_loop_guard_and_two_attempt_limit(self):
        workflow = self.load(GUARDIAN)
        text = json.dumps(workflow)
        self.assertIn("repair_attempts", text)
        self.assertIn("QUARANTINE", text)
        self.assertIn("WAKE_COLLECTOR", text)
        self.assertIn("STOP_AND_REQUEUE", text)
        self.assertIn("deepseek/deepseek-v4-flash", text)
        self.assertNotIn("CLAUDE", text.upper())
        self.assertEqual(workflow["settings"]["errorWorkflow"], "N5FeJODzdzqcWSjp")

    def test_paid_layer_three_is_guardian_only_and_red_flag_gated(self):
        collector = self.load(COLLECTOR)
        guardian = self.load(GUARDIAN)
        self.assertNotIn("deepseek/deepseek-v4-flash", json.dumps(collector))
        guardian_text = json.dumps(guardian)
        self.assertIn("Paid Escalation Needed?", guardian_text)
        self.assertIn("Paid Layer 3 Repair Supervisor", guardian_text)
        self.assertIn("paid_repair_needed", guardian_text)
        self.assertIn("STOP_AND_REQUEUE", guardian_text)
        self.assertIn("FREE_MODEL_UNAVAILABLE", guardian_text)
        self.assertIn("max_tokens", guardian_text)

    def test_guardian_reads_one_snapshot_rpc(self):
        workflow = self.load(GUARDIAN)
        text = json.dumps(workflow)
        self.assertIn("nexa_guardian_snapshot", text)
        self.assertIn("AGENARYS-NEXA-Supabase-ServiceRole", text)
        self.assertNotIn("NEXA_SUPABASE_SERVICE_ROLE_KEY", text)

    def test_webhook_wakes_use_existing_header_credential(self):
        collector = self.load(COLLECTOR)
        guardian = self.load(GUARDIAN)
        collector_hook = next(node for node in collector["nodes"] if node["name"] == "Authenticated Wake Webhook")
        guardian_hook = next(node for node in guardian["nodes"] if node["name"] == "Authenticated Guardian Wake Webhook")
        wake = next(node for node in guardian["nodes"] if node["name"] == "Wake Collector")
        self.assertEqual(collector_hook["parameters"]["authentication"], "headerAuth")
        self.assertEqual(guardian_hook["parameters"]["authentication"], "headerAuth")
        self.assertEqual(wake["parameters"]["authentication"], "genericCredentialType")
        self.assertNotIn("AGENARYS_INTERNAL_WORKFLOW_KEY", json.dumps([collector, guardian]))

    def test_supabase_requests_use_n8n_json_body_parameter(self):
        for workflow_name in (COLLECTOR, GUARDIAN):
            workflow = self.load(workflow_name)
            for item in workflow["nodes"]:
                if "ehzxnnxyliapvcvzzmyp.supabase.co" not in item.get("parameters", {}).get("url", ""):
                    continue
                parameters = item["parameters"]
                self.assertEqual(parameters.get("contentType"), "json", item["name"])
                self.assertIn("jsonBody", parameters, item["name"])
                self.assertNotIn("body", parameters, item["name"])

    def test_scalar_rpc_responses_are_read_as_text(self):
        guardian = self.load(GUARDIAN)
        for name in ("Write Guardian Heartbeat", "Release Stale Lease"):
            item = next(node for node in guardian["nodes"] if node["name"] == name)
            response = item["parameters"]["options"]["response"]["response"]
            self.assertEqual(response["responseFormat"], "text")

    def test_guardian_http_bodies_do_not_use_complex_object_expressions(self):
        guardian = self.load(GUARDIAN)
        for item in guardian["nodes"]:
            body = item.get("parameters", {}).get("jsonBody", "")
            self.assertNotIn("JSON.stringify({", body, item["name"])

    def test_incident_payload_survives_repair_http_nodes(self):
        guardian = self.load(GUARDIAN)
        self.assertTrue(any(node["name"] == "Build Incident Record Payload" for node in guardian["nodes"]))
        item = next(node for node in guardian["nodes"] if node["name"] == "Record Guardian Incident")
        self.assertIn("JSON.stringify($json)", item["parameters"]["jsonBody"])

    def test_guardian_strips_internal_execution_prefix_before_n8n_stop(self):
        workflow = self.load(GUARDIAN)
        stop = next(node for node in workflow["nodes"] if node["name"] == "Stop Looping Execution")
        self.assertIn("replace(/^n8n-/", stop["parameters"]["url"])

    def test_exports_contain_no_secret_values(self):
        for path in (N8N_DIR / COLLECTOR, N8N_DIR / GUARDIAN):
            text = path.read_text(encoding="utf-8")
            self.assertIsNone(re.search(r"sk-[A-Za-z0-9_-]{20,}", text))
            self.assertNotIn("SUPABASE_SERVICE_ROLE_KEY=", text)
            self.assertNotIn("Bearer eyJ", text)


if __name__ == "__main__":
    unittest.main()
