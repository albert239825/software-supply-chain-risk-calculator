import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { getPackageById } from "@/lib/db/packages";
import { listVersionsByPackage } from "@/lib/db/versions";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse, Version } from "@/types/api";

/**
 * R1 GET /api/packages/:id/versions — see `docs/api-spec.md` §R1 (Q1).
 *
 * Lists versions of a package ordered `released DESC NULLS LAST`.
 */
const querySchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(50),
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

    const { items, total } = await listVersionsByPackage(client, {
      packageId: pkg.id,
      ecosystem,
      limit,
      offset,
    });
    const body: ListResponse<Version> = {
      items,
      meta: { total, limit, offset },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
