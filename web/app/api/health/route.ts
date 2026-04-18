import { createClient } from "@supabase/supabase-js";

/**
 * A8 GET /api/health — liveness + Supabase connectivity probe.
 *
 * See docs/api-spec.md §A8. Behavior:
 *   - 200 { status: "ok", db: "unconfigured", time } if SUPABASE_URL or
 *     SUPABASE_SERVICE_ROLE_KEY is unset (local scaffold / CI without creds).
 *   - 200 { status: "ok", db: "ok", time } if a trivial probe succeeds.
 *   - 503 { status: "error", db: "error", time, error } if the probe fails.
 */
export async function GET() {
  const time = new Date().toISOString();
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    return Response.json({ status: "ok", db: "unconfigured", time });
  }

  try {
    const client = createClient(url, key, {
      auth: { persistSession: false, autoRefreshToken: false },
    });
    // Trivial connectivity probe. We don't assume any specific table exists
    // yet; calling an RPC with a non-existent name round-trips to Postgres
    // and surfaces connection/auth errors without requiring schema setup.
    // A "function not found" response (HTTP 404 from PostgREST) still means
    // the database is reachable, so we treat that as a successful probe.
    const { error } = await client.rpc("__supply_chain_risk_scorer_health__");
    if (error && error.code && error.code !== "PGRST202") {
      return Response.json(
        { status: "error", db: "error", time, error: error.message },
        { status: 503 },
      );
    }
    return Response.json({ status: "ok", db: "ok", time });
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return Response.json(
      { status: "error", db: "error", time, error: message },
      { status: 503 },
    );
  }
}
