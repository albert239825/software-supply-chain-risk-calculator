import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { listDependents, type DependentRow } from "@/lib/db/dependencies";
import { getPackageById } from "@/lib/db/packages";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * A5 GET /api/packages/:id/dependents — see `docs/api-spec.md` §A5.
 *
 * Lists packages that declare this package as a direct dependency. Deduped
 * by `(from_package, from_version, dep_kind)`.
 */
const querySchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(100),
  offset: z.coerce.number().int().min(0).default(0),
  ecosystem: ecosystemSchema,
});

const idSchema = z.uuid();

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> },
): Promise<NextResponse> {
  const { id } = await params;
  const idResult = idSchema.safeParse(id);
  if (!idResult.success) {
    return jsonError(400, "id must be a valid UUID");
  }

  const parsed = parseQuery(new URL(request.url), querySchema);
  if (parsed.error) return parsed.error;
  const { limit, offset, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const pkg = await getPackageById(client, { id: idResult.data, ecosystem });
    if (!pkg) return jsonError(404, "Package not found");

    const { items, total } = await listDependents(client, {
      packageId: pkg.id,
      ecosystem,
      limit,
      offset,
    });
    const body: ListResponse<DependentRow> = {
      items,
      meta: { total, limit, offset },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
