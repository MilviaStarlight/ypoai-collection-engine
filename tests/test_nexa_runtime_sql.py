from pathlib import Path
import unittest


SQL = Path("sql/20260713_nexa_collector_runtime.sql")
ROLLBACK = Path("sql/20260713_nexa_collector_runtime_rollback.sql")


class NexaRuntimeSqlTests(unittest.TestCase):
    def test_runtime_sql_is_private_reversible_and_bounded(self):
        text = SQL.read_text(encoding="utf-8").lower()
        rollback = ROLLBACK.read_text(encoding="utf-8").lower()
        for table in (
            "nexa_collector_control",
            "nexa_collector_heartbeats",
            "nexa_target_leases",
            "nexa_collector_incidents",
        ):
            self.assertIn(
                f"create table if not exists business_intelligence.{table}", text
            )
            self.assertIn(
                f"alter table business_intelligence.{table} enable row level security",
                text,
            )
            self.assertIn(
                f"drop table if exists business_intelligence.{table}", rollback
            )
        self.assertIn("revoke all", text)
        self.assertIn("grant select, insert, update, delete on", text)
        self.assertIn("to service_role", text)
        self.assertIn("greatest(least(p_limit, 10), 1)", text)
        self.assertIn("pg_advisory_xact_lock", text)
        self.assertIn("v_active_leases", text)
        self.assertIn("v_max_concurrency", text)
        self.assertIn("v_max_concurrency - v_active_leases", text)
        self.assertNotIn("else 'official_public_source'", text)
        self.assertIn(
            "function business_intelligence.nexa_guardian_snapshot()", text
        )
        for wrapper in (
            "nexa_read_collector_control",
            "nexa_claim_targets",
            "nexa_write_heartbeat",
            "nexa_requeue_target",
            "nexa_release_stale_lease",
            "nexa_record_incident",
            "nexa_guardian_snapshot",
        ):
            self.assertIn(f"function public.{wrapper}", text)
        self.assertNotIn("security definer", text)
        self.assertIn(
            "function if exists business_intelligence.nexa_guardian_snapshot()",
            rollback,
        )
        self.assertIn("function if exists business_intelligence.nexa_requeue_target", rollback)
        self.assertNotIn("grant all to anon", text)
        self.assertNotIn("grant all to authenticated", text)


if __name__ == "__main__":
    unittest.main()
