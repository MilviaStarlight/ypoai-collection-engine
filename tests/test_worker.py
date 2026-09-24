import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import worker  # noqa: E402


class TestWorkerPureFunctions(unittest.TestCase):
    def test_every_selector_is_well_formed(self):
        for slug, sels in worker.CATEGORY_SELECTORS.items():
            self.assertTrue(sels, slug)
            for s in sels:
                self.assertTrue(s.startswith("[") and s.endswith("]"), f"{slug}: {s}")

    def test_query_uses_state_iso_and_county_relation(self):
        q = worker.build_overpass_query("OH", "Franklin County", worker.CATEGORY_SELECTORS["restaurant"])
        self.assertIn('["ISO3166-2"="US-OH"]', q)
        self.assertIn('["name"="Franklin County"]', q)
        self.assertIn('nwr["amenity"="restaurant"](area.c);', q)
        self.assertIn("out center tags", q)

    def test_dc_uses_state_area_directly(self):
        q = worker.build_overpass_query("DC", "District of Columbia", worker.CATEGORY_SELECTORS["dentist"])
        self.assertIn('["ISO3166-2"="US-DC"]->.c', q)
        self.assertNotIn("map_to_area", q)

    def test_county_suffix_detection(self):
        self.assertTrue(worker.county_has_suffix("Franklin County"))
        self.assertTrue(worker.county_has_suffix("Orleans Parish"))
        self.assertTrue(worker.county_has_suffix("Juneau City and Borough"))
        self.assertTrue(worker.county_has_suffix("Richmond city"))
        self.assertFalse(worker.county_has_suffix("Alameda"))

    def test_normalize_element_requires_name(self):
        self.assertIsNone(worker.normalize_element({"type": "node", "id": 1, "tags": {"amenity": "restaurant"}},
                                                   category="restaurant", state="OH", county="Franklin County"))

    def test_normalize_element_maps_fields(self):
        el = {"type": "way", "id": 42, "center": {"lat": 39.9, "lon": -83.0}, "tags": {
            "name": "Buckeye Diner", "amenity": "restaurant", "cuisine": "american", "phone": "+1 614-555-0100",
            "website": "buckeyediner.com", "email": "hi@buckeyediner.com", "addr:housenumber": "12",
            "addr:street": "High St", "addr:city": "Columbus", "addr:postcode": "43215"}}
        row = worker.normalize_element(el, category="restaurant", state="OH", county="Franklin County")
        self.assertEqual(row["business_name"], "Buckeye Diner")
        self.assertEqual(row["source_url"], "https://www.openstreetmap.org/way/42")
        self.assertEqual(row["source_identifier"], "osm:way:42")
        self.assertEqual(row["website_url"], "https://buckeyediner.com")
        self.assertEqual(row["address_line1"], "12 High St")
        self.assertEqual(row["city"], "Columbus")
        self.assertEqual(row["postal_code"], "43215")
        self.assertEqual(row["state"], "OH")
        self.assertEqual(row["county"], "Franklin County")
        self.assertEqual(row["cuisine"], "american")
        self.assertTrue(row["private_review_required"])

    def test_normalize_rejects_bad_email(self):
        el = {"type": "node", "id": 7, "tags": {"name": "X", "email": "not-an-email"}}
        row = worker.normalize_element(el, category="restaurant", state="OH", county="Franklin County")
        self.assertIsNone(row["email"])

    def test_dedupe_by_source_identifier(self):
        a = {"source_identifier": "osm:node:1"}
        b = {"source_identifier": "osm:node:1"}
        c = {"source_identifier": "osm:node:2"}
        self.assertEqual(len(worker.dedupe([a, b, c])), 2)


if __name__ == "__main__":
    unittest.main()
