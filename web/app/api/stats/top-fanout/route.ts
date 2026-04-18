import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { topFanout, type TopFanoutRow } from "@/lib/db/rankings";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * R2 GET /api/stats/top-fanout — see `docs/api-spec.md` §R2 (Q2).
 *
 * Returns the top-N packages by direct dependency count. No pagination in
 * Phase 1 — `limit` caps at 100 and the response `meta.total` reflects the
 * number of returned rows rather than the ecosystem-wide total (per task
 * spec; a full fan-out census would require a separate count query).
 */
const topFanoutQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(100).default(10),
  ecosystem: ecosystemSchema,
});

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), topFanoutQuerySchema);
  if (parsed.error) return parsed.error;
  const { limit, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const items = await topFanout(client, { ecosystem, limit });
    const body: ListResponse<TopFanoutRow> = {
      items,
      meta: { total: items.length, limit, offset: 0 },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
