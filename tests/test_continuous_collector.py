import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "continuous_collector.py"
SPEC = importlib.util.spec_from_file_location("continuous_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["continuous_collector"] = collector
SPEC.loader.exec_module(collector)


class ContinuousCollectorTests(unittest.TestCase):
    def test_ar_is_deprioritized(self):
        ar_rank = collector.target_rank(
            {"state": "AR", "county": "Pulaski", "category_slug": "dentist", "target_goal": 6, "collected_count": 3, "cycle_status": "pending"},
            worker_slot=1,
        )
        ca_rank = collector.target_rank(
            {"state": "CA", "county": "Orange County", "category_slug": "dentist", "target_goal": 6, "collected_count": 3, "cycle_status": "pending"},
            worker_slot=1,
        )
        self.assertGreater(ar_rank[0], ca_rank[0])

    def test_state_family_resolution(self):
        self.assertEqual(collector.source_family_for_category("urgent-care"), "npi_registry")
        self.assertEqual(collector.source_family_for_category("restaurant"), "restaurant_food")
        self.assertEqual(collector.source_family_for_category("childcare"), "childcare_license")

    def test_stable_bucket_is_stable(self):
        self.assertEqual(collector.stable_bucket("AL:Jefferson:dentist", 8), collector.stable_bucket("AL:Jefferson:dentist", 8))


if __name__ == "__main__":
    unittest.main()
