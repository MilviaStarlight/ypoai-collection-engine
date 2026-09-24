type JwtClaims = {
  role?: unknown;
  ref?: unknown;
};

function decodeClaims(token: string): JwtClaims | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    const normalized = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
    return JSON.parse(atob(padded)) as JwtClaims;
  } catch {
    return null;
  }
}

export function isAuthorizedEdgeRequest(
  request: Request,
  runtimeServiceRoleKey: string,
  internalWorkflowKey: string,
  projectId: string,
): boolean {
  const authorization = request.headers.get("authorization") ?? "";
  const apikey = request.headers.get("apikey") ?? "";
  const suppliedWorkflowKey = request.headers.get("x-agenarys-workflow-key") ?? "";

  if (
    authorization === `Bearer ${runtimeServiceRoleKey}`
    || apikey === runtimeServiceRoleKey
  ) {
    return true;
  }

  if (
    internalWorkflowKey.length > 0
    && suppliedWorkflowKey === internalWorkflowKey
  ) {
    return true;
  }

  if (!authorization.startsWith("Bearer ")) return false;
  const claims = decodeClaims(authorization.slice("Bearer ".length));
  return claims?.role === "service_role" && claims?.ref === projectId;
}
