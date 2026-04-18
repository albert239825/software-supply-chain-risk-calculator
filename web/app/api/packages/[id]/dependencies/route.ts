import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import {
  depKindSchema,
  ecosystemSchema,
  jsonError,
  parseQuery,
} from "@/lib/api/params";
import {
  listDirectDependencies,
  type DirectDependencyRow,
} from "@/lib/db/dependencies";
import { getPackageById } from "@/lib/db/packages";
import { getLatestVersionRecord } from "@/lib/db/versions";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse } from "@/types/api";

/**
 * A4 GET /api/packages/:id/dependencies — see `docs/api-spec.md` §A4.
 *
 * Returns the direct dependencies of the package's latest version. 404s
 * when the package is unknown OR when the package has no matching latest
 * version row in `versions`.
 */
const querySchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(100),
  offset: z.coerce.number().int().min(0).default(0),
  ecosystem: ecosystemSchema,
  depKind: depKindSchema,
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
  const { limit, offset, ecosystem, depKind } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const pkg = await getPackageById(client, { id: idResult.data, ecosystem });
    if (!pkg) return jsonError(404, "Package not found");

    const latest = await getLatestVersionRecord(client, {
      packageId: pkg.id,
      ecosystem,
      latestVersion: pkg.latest_version,
    });
    if (!latest) return jsonError(404, "Package not found");

    const { items, total } = await listDirectDependencies(client, {
      packageId: pkg.id,
      ecosystem,
      depKind,
      limit,
      offset,
      fromVersionId: latest.id,
    });
    const body: ListResponse<DirectDependencyRow> = {
      items,
      meta: { total, limit, offset },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
