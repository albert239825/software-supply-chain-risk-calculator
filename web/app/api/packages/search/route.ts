import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { searchPackagesByName } from "@/lib/db/packages";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse, Package } from "@/types/api";

/**
 * A1 GET /api/packages/search — see `docs/api-spec.md` §A1.
 *
 * Query params:
 *   - q          : string (required, min length 1)
 *   - limit      : int   (default 10, max 50)
 *   - ecosystem  : "npm" | "pypi" (default "npm")
 *
 * Route handler responsibilities are limited to: (1) validate query params,
 * (2) dispatch to `lib/db/packages`, (3) shape the response. No SQL lives
 * here.
 */
const searchQuerySchema = z.object({
  q: z.string().min(1, "q is required"),
  limit: z.coerce.number().int().positive().max(50).default(10),
  ecosystem: ecosystemSchema,
});

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), searchQuerySchema);
  if (parsed.error) return parsed.error;
  const { q, limit, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const items = await searchPackagesByName(client, { q, ecosystem, limit });
    const body: ListResponse<Package> = {
      items,
      meta: { total: items.length, limit, offset: 0 },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
