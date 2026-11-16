import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import overture_extract as ox  # noqa: E402
import worker  # noqa: E402


class CollectionScopeTests(unittest.TestCase):
    def test_osm_normalizer_suppresses_email_and_website(self):
        row = worker.normalize_element(
            {
                "type": "node",
                "id": "scope-fixture",
                "lat": 18.0,
                "lon": -66.0,
                "tags": {
                    "name": "Scope Fixture",
                    "phone": "555-0100",
                    "email": "contact@example.test",
                    "website": "https://example.test",
                    "addr:housenumber": "10",
                    "addr:street": "Main St",
                    "addr:city": "Sampletown",
                    "addr:postcode": "00000",
                },
            },
            category="restaurant",
            state="PR",
            county="Sample County",
        )

        self.assertIsNone(row["email"])
        self.assertIsNone(row["website_url"])
        self.assertEqual(row["phone"], "555-0100")
        self.assertEqual(row["address_line1"], "10 Main St")

    def test_overture_normalizer_suppresses_email_and_website(self):
        row = ox.to_row(
            {
                "id": "scope-fixture",
                "names": {"primary": "Scope Fixture"},
                "phones": ["555-0100"],
                "emails": ["contact@example.test"],
                "websites": ["https://example.test"],
                "addresses": [{"freeform": "10 Main St", "locality": "Sampletown", "postcode": "00000"}],
                "bbox": {"xmin": -66.1, "ymin": 17.9, "xmax": -65.9, "ymax": 18.1},
            },
            category="restaurant",
            state="PR",
            county="Sample County",
            release="fixture-release",
        )

        self.assertIsNone(row["email"])
        self.assertIsNone(row["website_url"])
        self.assertEqual(row["phone"], "555-0100")
        self.assertEqual(row["address_line1"], "10 Main St")

    def test_overture_reader_does_not_request_contact_columns(self):
        class Expr:
            def __ge__(self, other):
                return self

            def __le__(self, other):
                return self

            def __and__(self, other):
                return self

        class Dataset:
            requested_columns = None

            def to_batches(self, *, columns, filter):
                self.requested_columns = columns
                return []

        dataset = Dataset()
        pyarrow = types.ModuleType("pyarrow")
        pyarrow.__path__ = []
        compute = types.ModuleType("pyarrow.compute")
        compute.field = lambda *args: Expr()
        arrow_dataset = types.ModuleType("pyarrow.dataset")
        arrow_dataset.dataset = lambda *args, **kwargs: dataset
        fs = types.ModuleType("pyarrow.fs")
        fs.S3FileSystem = lambda **kwargs: object()
        pyarrow.compute = compute
        pyarrow.dataset = arrow_dataset
        pyarrow.fs = fs

        with patch.dict(
            sys.modules,
            {"pyarrow": pyarrow, "pyarrow.compute": compute, "pyarrow.dataset": arrow_dataset, "pyarrow.fs": fs},
        ):
            list(ox.read_places("fixture-release", (-67.0, 17.0, -65.0, 19.0)))

        self.assertNotIn("websites", dataset.requested_columns)
        self.assertNotIn("emails", dataset.requested_columns)


if __name__ == "__main__":
    unittest.main()
