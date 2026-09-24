import assert from "node:assert/strict";
import test from "node:test";

import {
  dedupeRows,
  normalizeNominatim,
  normalizeNpi,
  normalizePhoton,
  type ClaimedTarget,
} from "./nexa_source_normalizers.ts";


const target: ClaimedTarget = {
  id: "00000000-0000-0000-0000-000000000001",
  state: "FL",
  county: "Miami-Dade County",
  category_slug: "dentist",
  collected_count: 0,
  target_goal: 10,
};


test("normalizes an organization NPI with private-review provenance", () => {
  const raw = {
    number: "1104969039",
    basic: { organization_name: "183 DENTAL GROUP, PA" },
    addresses: [
      {
        address_purpose: "LOCATION",
        country_code: "US",
        address_1: "636 NW 183RD ST",
        city: "MIAMI",
        state: "FL",
        postal_code: "331694470",
        telephone_number: "3056528338",
      },
    ],
    authorization: "must-not-survive",
  };

  const row = normalizeNpi(raw, target, "2026-07-13T18:00:00.000Z");

  assert.ok(row);
  assert.equal(row.business_name, "183 DENTAL GROUP, PA");
  assert.equal(row.phone, "305-652-8338");
  assert.equal(row.city, "Miami");
  assert.equal(row.source_identifier, "1104969039");
  assert.equal(row.publication_status, "private_review_required");
  assert.equal(row.provenance.source_name, "NPPES NPI Registry API");
  assert.equal("authorization" in row, false);
});


test("deduplicates NPI rows by source identifier", () => {
  const first = normalizeNpi(
    {
      number: "1104969039",
      basic: { organization_name: "FIRST NAME" },
      addresses: [{ address_purpose: "LOCATION", country_code: "US", state: "FL" }],
    },
    target,
    "2026-07-13T18:00:00.000Z",
  );
  const duplicate = normalizeNpi(
    {
      number: "1104969039",
      basic: { organization_name: "DUPLICATE NAME" },
      addresses: [{ address_purpose: "LOCATION", country_code: "US", state: "FL" }],
    },
    target,
    "2026-07-13T18:00:01.000Z",
  );

  assert.equal(dedupeRows([first!, duplicate!]).length, 1);
  assert.equal(dedupeRows([first!, duplicate!])[0].business_name, "FIRST NAME");
});


test("normalizes Photon output with source provenance", () => {
  const row = normalizePhoton(
    {
      properties: {
        osm_id: 123,
        name: "Sunrise Cafe",
        street: "Main Street",
        housenumber: "10",
        city: "Miami",
        county: "Miami-Dade County",
        state: "Florida",
        statecode: "FL",
        postcode: "33101",
      },
    },
    { ...target, category_slug: "cafe" },
    "2026-07-13T18:00:00.000Z",
  );

  assert.ok(row);
  assert.equal(row.business_name, "Sunrise Cafe");
  assert.equal(row.provenance.source_name, "OpenStreetMap Photon");
  assert.match(row.source_url, /photon\.komoot\.io/);
});


test("rejects an OSM row from the wrong state", () => {
  const row = normalizeNominatim(
    {
      osm_id: 987,
      display_name: "Wrong State Cafe",
      name: "Wrong State Cafe",
      address: { state: "Georgia", "ISO3166-2-lvl4": "US-GA" },
    },
    { ...target, category_slug: "cafe" },
    "2026-07-13T18:00:00.000Z",
  );

  assert.equal(row, null);
});

test("rejects an OSM row from the wrong county", () => {
  const row = normalizePhoton(
    {
      properties: {
        osm_id: 456,
        name: "Other County Cafe",
        statecode: "FL",
        county: "Broward County",
      },
    },
    { ...target, category_slug: "cafe" },
    "2026-07-13T18:00:00.000Z",
  );

  assert.equal(row, null);
});


test("rejects a source row without a business name", () => {
  const row = normalizePhoton(
    { properties: { osm_id: 123, statecode: "FL" } },
    target,
    "2026-07-13T18:00:00.000Z",
  );

  assert.equal(row, null);
});
