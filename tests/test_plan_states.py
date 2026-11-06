import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import plan_states as ps  # noqa: E402


class PlanStatesTest(unittest.TestCase):
    def test_groups_cover_every_state_once(self):
        flat = [s for g in ps.groups() for s in g]
        self.assertEqual(sorted(flat), sorted(ps.ALL))
        self.assertEqual(len(flat), len(set(flat)))
        self.assertEqual(len(ps.ALL), 52)

    def test_schedule_rotates_through_six_groups_per_day(self):
        seen = [tuple(ps.plan("schedule", "", h)) for h in range(0, 24, 4)]
        self.assertEqual(len(set(seen)), 6)
        self.assertEqual(ps.plan("schedule", "", 1)[0], "PR")

    def test_manual_inputs(self):
        self.assertEqual(ps.plan("workflow_dispatch", "pr, fl, xx", 0), ["PR", "FL"])
        self.assertEqual(ps.plan("workflow_dispatch", "launch", 0), ps.LAUNCH)
        self.assertEqual(ps.plan("workflow_dispatch", "all", 0), ps.ALL)


if __name__ == "__main__":
    unittest.main()
