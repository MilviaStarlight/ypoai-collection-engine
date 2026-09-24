import assert from "node:assert/strict";
import test from "node:test";

import { isAuthorizedEdgeRequest } from "./edge_auth.ts";


function token(claims: Record<string, unknown>): string {
  const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "HS256", typ: "JWT" })}.${encode(claims)}.test-signature`;
}

function requestWith(bearer: string) {
  return new Request("https://example.test", {
    headers: { authorization: `Bearer ${bearer}` },
  });
}

test("accepts a gateway-verified service-role JWT for the exact project", () => {
  const jwt = token({ role: "service_role", ref: "ehzxnnxyliapvcvzzmyp" });
  assert.equal(
    isAuthorizedEdgeRequest(requestWith(jwt), "different-runtime-key", "", "ehzxnnxyliapvcvzzmyp"),
    true,
  );
});

test("rejects an anon JWT", () => {
  const jwt = token({ role: "anon", ref: "ehzxnnxyliapvcvzzmyp" });
  assert.equal(
    isAuthorizedEdgeRequest(requestWith(jwt), "different-runtime-key", "", "ehzxnnxyliapvcvzzmyp"),
    false,
  );
});

test("rejects a service-role JWT from another project", () => {
  const jwt = token({ role: "service_role", ref: "another-project" });
  assert.equal(
    isAuthorizedEdgeRequest(requestWith(jwt), "different-runtime-key", "", "ehzxnnxyliapvcvzzmyp"),
    false,
  );
});

test("accepts the optional internal workflow key only when configured", () => {
  const request = new Request("https://example.test", {
    headers: { "x-agenarys-workflow-key": "configured-key" },
  });
  assert.equal(
    isAuthorizedEdgeRequest(request, "runtime-key", "configured-key", "ehzxnnxyliapvcvzzmyp"),
    true,
  );
  assert.equal(
    isAuthorizedEdgeRequest(request, "runtime-key", "", "ehzxnnxyliapvcvzzmyp"),
    false,
  );
});
