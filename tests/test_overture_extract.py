import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import overture_extract as ox  # noqa: E402
import worker  # noqa: E402


class OvertureMappingTest(unittest.TestCase):
    def test_most_specific_level_wins(self):
        tx = {"primary": "barber", "hierarchy": ["lifestyle_services", "personal_or_beauty_service", "barber"]}
        self.assertEqual(ox.slug_for(tx, None), "barber")

    def test_restaurant_family_maps_through_hierarchy(self):
        tx = {"primary": "mexican_restaurant", "hierarchy": ["food_and_drink", "restaurant", "latin_american_restaurant", "mexican_restaurant"]}
        self.assertEqual(ox.slug_for(tx, None), "restaurant")

    def test_unmapped_returns_none(self):
        self.assertIsNone(ox.slug_for({"primary": "beach", "hierarchy": ["geographic_entities", "beach"]}, "beach"))

    def test_every_target_slug_exists(self):
        missing = sorted({s for s in ox.OVERTURE_TO_SLUG.values() if s not in worker.CATEGORY_SELECTORS})
        self.assertEqual(missing, [])

    def test_row_shape(self):
        p = {"id": "abc", "names": {"primary": "Panaderia La Ceiba"}, "phones": ["+17875551234"], "websites": ["laceiba.pr"],
             "emails": None, "addresses": [{"freeform": "Calle 1", "locality": "Dorado", "postcode": "00646"}],
             "bbox": {"xmin": -66.27, "xmax": -66.27, "ymin": 18.46, "ymax": 18.46}, "taxonomy": {"primary": "bakery"},
             "confidence": 0.9}
        r = ox.to_row(p, category="cafes-bakeries", state="PR", county="Dorado Municipio", release="2026-10-21.0")
        self.assertEqual(r["source_identifier"], "overture:abc")
        self.assertEqual(r["website_url"], "https://laceiba.pr")
        self.assertEqual(r["county"], "Dorado Municipio")
        self.assertEqual(r["city"], "Dorado")


if __name__ == "__main__":
    unittest.main()
