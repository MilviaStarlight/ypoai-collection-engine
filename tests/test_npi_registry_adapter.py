import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "adapters" / "npi_registry_adapter.py"
SPEC = importlib.util.spec_from_file_location("npi_registry_adapter", MODULE_PATH)
npi = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["npi_registry_adapter"] = npi
SPEC.loader.exec_module(npi)


class NpiRegistryAdapterTests(unittest.TestCase):
    def test_normalize_npi_result_for_organization(self):
        result = {
            "number": "1104969039",
            "enumeration_type": "NPI-2",
            "basic": {
                "organization_name": "183 DENTAL GROUP, PA",
                "status": "A",
                "enumeration_date": "2007-02-14",
                "last_updated": "2008-06-13",
            },
            "addresses": [
                {
                    "address_purpose": "LOCATION",
                    "country_code": "US",
                    "address_1": "636 NW 183RD ST",
                    "city": "MIAMI",
                    "state": "FL",
                    "postal_code": "331694470",
                    "telephone_number": "3056528338",
                }
            ],
            "taxonomies": [
                {
                    "code": "1223G0001X",
                    "desc": "Dentist, General Practice",
                    "license": "DN 12408",
                    "primary": True,
                    "state": "FL",
                }
            ],
        }

        row = npi.normalize_npi_result(result, "dentist")

        self.assertEqual(row["business_name"], "183 DENTAL GROUP, PA")
        self.assertEqual(row["phone"], "305-652-8338")
        self.assertEqual(row["city"], "Miami")
        self.assertEqual(row["source_identifier"], "1104969039")
        self.assertTrue(row["source_url"].endswith("number=1104969039"))
        self.assertEqual(row["notes"], npi.PRIVATE_REVIEW_NOTE)
        self.assertEqual(row["publication_status"], "private_review_required")

    def test_normalize_npi_result_for_individual(self):
        result = {
            "number": "1234567890",
            "enumeration_type": "NPI-1",
            "basic": {
                "first_name": "JANE",
                "last_name": "RIVERA",
                "credential": "MD",
                "status": "A",
            },
            "addresses": [
                {
                    "address_purpose": "LOCATION",
                    "country_code": "US",
                    "address_1": "10 MAIN ST",
                    "city": "ORLANDO",
                    "state": "FL",
                    "postal_code": "32801",
                    "telephone_number": "4075550100",
                }
            ],
            "taxonomies": [
                {
                    "code": "207Q00000X",
                    "desc": "Family Medicine Physician",
                    "primary": True,
                    "state": "FL",
                }
            ],
        }

        row = npi.normalize_npi_result(result, "doctor")

        self.assertEqual(row["business_name"], "JANE RIVERA MD")
        self.assertEqual(row["phone"], "407-555-0100")
        self.assertEqual(row["taxonomy_description"], "Family Medicine Physician")
        self.assertTrue(row["private_review_required"])

    def test_dedupe_rows_prefers_npi_number(self):
        rows = [
            {"business_name": "A", "dedupe_keys": {"npi": "1"}},
            {"business_name": "A duplicate", "dedupe_keys": {"npi": "1"}},
            {"business_name": "B", "dedupe_keys": {"npi": "2"}},
        ]

        self.assertEqual([row["business_name"] for row in npi.dedupe_rows(rows)], ["A", "B"])


if __name__ == "__main__":
    unittest.main()
