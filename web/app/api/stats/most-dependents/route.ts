import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { mostDependents, type MostDependentsRow } from "@/lib/db/rankings";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * R5 GET /api/stats/most-dependents — see `docs/api-spec.md` §R5 (Q5).
 *
 * Returns packages most frequently depended on (ecosystem blast-radius).
 * No pagination in Phase 1; `meta.total` mirrors `items.length`.
 */
const mostDependentsQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(100).default(10),
  ecosystem: ecosystemSchema,
});

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), mostDependentsQuerySchema);
  if (parsed.error) return parsed.error;
  const { limit, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const items = await mostDependents(client, { ecosystem, limit });
    const body: ListResponse<MostDependentsRow> = {
      items,
      meta: { total: items.length, limit, offset: 0 },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
