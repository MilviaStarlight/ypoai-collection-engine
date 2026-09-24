export type ClaimedTarget = {
  id: string;
  state: string;
  county: string;
  category_slug: string;
  collected_count: number;
  target_goal: number;
};

export type NormalizedBusinessRow = {
  business_name: string;
  source_url: string;
  source_identifier: string | null;
  website_url: string | null;
  phone: string | null;
  email: string | null;
  address_line1: string | null;
  city: string | null;
  postal_code: string | null;
  notes: "Private review required before publication.";
  publication_status: "private_review_required";
  provenance: {
    source_name: string;
    source_url: string;
    collected_at: string;
  };
};

type JsonRecord = Record<string, unknown>;

const PRIVATE_REVIEW_NOTE = "Private review required before publication." as const;

const STATE_CODES: Record<string, string> = {
  alabama: "AL", alaska: "AK", arizona: "AZ", arkansas: "AR",
  california: "CA", colorado: "CO", connecticut: "CT", delaware: "DE",
  florida: "FL", georgia: "GA", hawaii: "HI", idaho: "ID",
  illinois: "IL", indiana: "IN", iowa: "IA", kansas: "KS",
  kentucky: "KY", louisiana: "LA", maine: "ME", maryland: "MD",
  massachusetts: "MA", michigan: "MI", minnesota: "MN", mississippi: "MS",
  missouri: "MO", montana: "MT", nebraska: "NE", nevada: "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
  "new york": "NY", "north carolina": "NC", "north dakota": "ND",
  ohio: "OH", oklahoma: "OK", oregon: "OR", pennsylvania: "PA",
  "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
  tennessee: "TN", texas: "TX", utah: "UT", vermont: "VT",
  virginia: "VA", washington: "WA", "west virginia": "WV",
  wisconsin: "WI", wyoming: "WY", "district of columbia": "DC",
};

function record(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : {};
}

function stringValue(value: unknown): string {
  return typeof value === "string" || typeof value === "number"
    ? String(value).replace(/\s+/g, " ").trim()
    : "";
}

function nullable(value: unknown): string | null {
  return stringValue(value) || null;
}

function titleCase(value: unknown): string | null {
  const clean = stringValue(value);
  if (!clean) return null;
  if (clean !== clean.toUpperCase()) return clean;
  return clean.toLowerCase().replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function normalizePhone(value: unknown): string | null {
  const digits = stringValue(value).replace(/\D/g, "");
  const local = digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;
  if (local.length === 10) {
    return `${local.slice(0, 3)}-${local.slice(3, 6)}-${local.slice(6)}`;
  }
  return local || null;
}

function normalizeState(value: unknown): string {
  const clean = stringValue(value).replace(/^US-/i, "").trim();
  if (/^[A-Za-z]{2}$/.test(clean)) return clean.toUpperCase();
  return STATE_CODES[clean.toLowerCase()] ?? "";
}

function sameState(rawState: unknown, target: ClaimedTarget): boolean {
  const state = normalizeState(rawState);
  return !state || state === target.state.toUpperCase();
}

function normalizeCounty(value: unknown): string {
  return stringValue(value)
    .toLowerCase()
    .replace(/\bcounty\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function sameCounty(rawCounty: unknown, target: ClaimedTarget): boolean {
  const county = normalizeCounty(rawCounty);
  return Boolean(county) && county === normalizeCounty(target.county);
}

function joinAddress(...parts: unknown[]): string | null {
  const clean = parts.map(stringValue).filter(Boolean);
  return clean.length ? clean.join(" ") : null;
}

function npiName(raw: JsonRecord): string {
  const basic = record(raw.basic);
  const organization = stringValue(basic.organization_name);
  if (organization) return organization;
  return [
    basic.name_prefix,
    basic.first_name,
    basic.middle_name,
    basic.last_name,
    basic.name_suffix,
    basic.credential,
  ].map(stringValue).filter(Boolean).join(" ");
}

function npiLocation(raw: JsonRecord): JsonRecord {
  const addresses = Array.isArray(raw.addresses) ? raw.addresses.map(record) : [];
  return addresses.find((address) => (
    stringValue(address.address_purpose) === "LOCATION"
    && stringValue(address.country_code) === "US"
  )) ?? addresses.find((address) => stringValue(address.country_code) === "US") ?? addresses[0] ?? {};
}

function buildRow(
  fields: Omit<NormalizedBusinessRow, "notes" | "publication_status" | "provenance">,
  sourceName: string,
  collectedAt: string,
): NormalizedBusinessRow | null {
  if (!fields.business_name || !fields.source_url) return null;
  return {
    ...fields,
    notes: PRIVATE_REVIEW_NOTE,
    publication_status: "private_review_required",
    provenance: {
      source_name: sourceName,
      source_url: fields.source_url,
      collected_at: collectedAt,
    },
  };
}

export function normalizeNpi(
  value: unknown,
  target: ClaimedTarget,
  collectedAt = new Date().toISOString(),
): NormalizedBusinessRow | null {
  const raw = record(value);
  const address = npiLocation(raw);
  if (!sameState(address.state, target)) return null;
  const number = stringValue(raw.number);
  const sourceUrl = number
    ? `https://npiregistry.cms.hhs.gov/api/?version=2.1&number=${encodeURIComponent(number)}`
    : "";
  return buildRow({
    business_name: npiName(raw),
    source_url: sourceUrl,
    source_identifier: number || null,
    website_url: null,
    phone: normalizePhone(address.telephone_number),
    email: null,
    address_line1: joinAddress(address.address_1, address.address_2),
    city: titleCase(address.city),
    postal_code: nullable(address.postal_code),
  }, "NPPES NPI Registry API", collectedAt);
}

export function normalizePhoton(
  value: unknown,
  target: ClaimedTarget,
  collectedAt = new Date().toISOString(),
): NormalizedBusinessRow | null {
  const raw = record(value);
  const properties = record(raw.properties);
  const state = properties.statecode || properties.state;
  if (!sameState(state, target)) return null;
  if (!sameCounty(properties.county, target)) return null;
  const osmId = stringValue(properties.osm_id);
  const sourceUrl = osmId
    ? `https://photon.komoot.io/api/?osm_id=${encodeURIComponent(osmId)}`
    : "";
  return buildRow({
    business_name: stringValue(properties.name),
    source_url: sourceUrl,
    source_identifier: osmId || null,
    website_url: nullable(properties.website),
    phone: normalizePhone(properties.phone),
    email: nullable(properties.email),
    address_line1: joinAddress(properties.housenumber, properties.street),
    city: titleCase(properties.city || properties.locality),
    postal_code: nullable(properties.postcode),
  }, "OpenStreetMap Photon", collectedAt);
}

export function normalizeNominatim(
  value: unknown,
  target: ClaimedTarget,
  collectedAt = new Date().toISOString(),
): NormalizedBusinessRow | null {
  const raw = record(value);
  const address = record(raw.address);
  const state = address["ISO3166-2-lvl4"] || address.state_code || address.state;
  if (!sameState(state, target)) return null;
  if (!sameCounty(address.county, target)) return null;
  const osmId = stringValue(raw.osm_id);
  const osmType = stringValue(raw.osm_type) || "node";
  const sourceUrl = osmId
    ? `https://www.openstreetmap.org/${encodeURIComponent(osmType)}/${encodeURIComponent(osmId)}`
    : "";
  return buildRow({
    business_name: stringValue(raw.name || record(raw.namedetails).name || raw.display_name),
    source_url: sourceUrl,
    source_identifier: osmId || null,
    website_url: nullable(record(raw.extratags).website),
    phone: normalizePhone(record(raw.extratags).phone),
    email: nullable(record(raw.extratags).email),
    address_line1: joinAddress(address.house_number, address.road),
    city: titleCase(address.city || address.town || address.village),
    postal_code: nullable(address.postcode),
  }, "OpenStreetMap Nominatim", collectedAt);
}

export function dedupeRows(rows: NormalizedBusinessRow[]): NormalizedBusinessRow[] {
  const seen = new Set<string>();
  const output: NormalizedBusinessRow[] = [];
  for (const row of rows) {
    const key = row.source_identifier
      ? `${row.provenance.source_name}:${row.source_identifier}`
      : [row.business_name, row.phone, row.address_line1, row.website_url]
        .map((part) => (part ?? "").toUpperCase().replace(/[^A-Z0-9]/g, ""))
        .join(":");
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(row);
  }
  return output;
}
