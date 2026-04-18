import { NextResponse } from "next/server";

import { createSupabaseServerClient } from "@/lib/supabase";

/**
 * A8 GET /api/health — liveness + Supabase connectivity probe.
 *
 * See `docs/api-spec.md` §A8. Behavior:
 *   - 200 { status: "ok", db: "unconfigured", time } if SUPABASE_URL or
 *     SUPABASE_SERVICE_ROLE_KEY is unset (local scaffold / CI without creds).
 *   - 200 { status: "ok", db: "ok", time } if a trivial probe against the
 *     `packages` table succeeds.
 *   - 503 { status: "error", db: "error", time, error } otherwise.
 *
 * The probe is `SELECT id FROM packages` with `count: 'exact', head: true`
 * and `limit(0)`, which round-trips to Postgres without transferring any
 * row bodies.
 */
export async function GET(): Promise<NextResponse> {
  const time = new Date().toISOString();
  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return NextResponse.json({ status: "ok", db: "unconfigured", time });
  }

  try {
    const client = createSupabaseServerClient();
    const { error } = await client
      .from("packages")
      .select("id", { count: "exact", head: true })
      .limit(0);
    if (error) {
      return NextResponse.json(
        { status: "error", db: "error", time, error: error.message },
        { status: 503 },
      );
    }
    return NextResponse.json({ status: "ok", db: "ok", time });
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return NextResponse.json(
      { status: "error", db: "error", time, error: message },
      { status: 503 },
    );
  }
}
