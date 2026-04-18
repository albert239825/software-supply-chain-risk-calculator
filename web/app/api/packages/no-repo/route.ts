import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import {
  listVersionsMissingRepo,
  type VersionMissingRepoRow,
} from "@/lib/db/versions";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * R9 GET /api/packages/no-repo — see `docs/api-spec.md` §R9 (Q9).
 *
 * Lists package versions whose `has_repository` is null/empty or
 * lowercases to one of `'false' | '0' | 'no'`.
 *
 * Note: this is a literal `no-repo` route sibling to the dynamic
 * `[id]` segment. Next.js App Router resolves static segments before
 * dynamic ones, so requests to `/api/packages/no-repo` never fall through
 * to `/api/packages/[id]`.
 */
const querySchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(100),
  offset: z.coerce.number().int().min(0).default(0),
  ecosystem: ecosystemSchema,
});

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), querySchema);
  if (parsed.error) return parsed.error;
  const { limit, offset, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const { items, total } = await listVersionsMissingRepo(client, {
      ecosystem,
      limit,
      offset,
    });
    const body: ListResponse<VersionMissingRepoRow> = {
      items,
      meta: { total, limit, offset },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
