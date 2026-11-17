import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import requests  # noqa: E402
import state_extract  # noqa: E402


class _Resp:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, _n):
        yield b"abc"


class DownloadRetryTest(unittest.TestCase):
    def test_retries_then_succeeds(self):
        calls = [requests.ConnectionError("Network is unreachable"), requests.ConnectionError("again"), _Resp()]
        sleeps = []
        with tempfile.TemporaryDirectory() as d, mock.patch.object(state_extract.requests, "get", side_effect=calls) as get:
            dest = os.path.join(d, "x.pbf")
            state_extract.download("https://example.invalid/x", dest, sleep=sleeps.append)
            self.assertEqual(get.call_count, 3)
            self.assertEqual(sleeps, [20, 40])
            with open(dest, "rb") as f:
                self.assertEqual(f.read(), b"abc")

    def test_gives_up_after_all_attempts(self):
        err = requests.ConnectionError("down")
        with tempfile.TemporaryDirectory() as d, mock.patch.object(state_extract.requests, "get", side_effect=err) as get:
            with self.assertRaises(requests.ConnectionError):
                state_extract.download("https://example.invalid/x", os.path.join(d, "x"), sleep=lambda _s: None)
            self.assertEqual(get.call_count, state_extract.DOWNLOAD_ATTEMPTS)


if __name__ == "__main__":
    unittest.main()
