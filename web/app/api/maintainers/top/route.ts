import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { topMaintainers, type TopMaintainerRow } from "@/lib/db/rankings";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * R6 GET /api/maintainers/top — see `docs/api-spec.md` §R6 (Q6).
 *
 * Maintainers responsible for the most packages (trust-concentration).
 * No pagination in Phase 1; `meta.total` mirrors `items.length`.
 */
const topMaintainersQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(100).default(10),
  ecosystem: ecosystemSchema,
});

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), topMaintainersQuerySchema);
  if (parsed.error) return parsed.error;
  const { limit, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const items = await topMaintainers(client, { ecosystem, limit });
    const body: ListResponse<TopMaintainerRow> = {
      items,
      meta: { total: items.length, limit, offset: 0 },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
