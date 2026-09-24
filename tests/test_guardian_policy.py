from datetime import datetime, timedelta, timezone
import unittest

from scripts.guardian_policy import GuardianObservation, decide


NOW = datetime(2026, 7, 13, 18, 0, tzinfo=timezone.utc)


class GuardianPolicyTests(unittest.TestCase):
    def test_wakes_sleeping_collector_with_ready_work(self):
        observation = GuardianObservation(20, None, None, 0, 0, False, 0, None)
        self.assertEqual(decide(observation, NOW).action, "WAKE_COLLECTOR")

    def test_does_nothing_when_queue_is_empty(self):
        observation = GuardianObservation(0, None, None, 0, 0, False, 0, None)
        self.assertEqual(decide(observation, NOW).action, "NO_ACTION")

    def test_releases_expired_nonprogressing_lease(self):
        old = NOW - timedelta(minutes=30)
        observation = GuardianObservation(10, old, old, 1, 0, True, 0, "timeout")
        self.assertEqual(decide(observation, NOW).action, "RELEASE_STALE_LEASE")

    def test_stops_repeated_target_loop(self):
        recent = NOW - timedelta(minutes=1)
        observation = GuardianObservation(
            10, recent, recent, 3, 0, False, 0, "same-target"
        )
        self.assertEqual(decide(observation, NOW).action, "STOP_AND_REQUEUE")

    def test_quarantines_after_two_repairs(self):
        old = NOW - timedelta(minutes=30)
        observation = GuardianObservation(10, old, old, 3, 3, True, 2, "same-error")
        decision = decide(observation, NOW)
        self.assertTrue(decision.quarantine)
        self.assertEqual(decision.action, "QUARANTINE")

    def test_free_model_rate_limit_requeues_without_paid_action(self):
        recent = NOW - timedelta(minutes=1)
        observation = GuardianObservation(
            10, recent, recent, 0, 0, False, 1, "model-rate-limit"
        )
        decision = decide(observation, NOW)
        self.assertEqual(decision.action, "REQUEUE_FREE_MODEL")
        self.assertNotIn("PAID", decision.action)


if __name__ == "__main__":
    unittest.main()
