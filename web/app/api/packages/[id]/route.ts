import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { getPackageById } from "@/lib/db/packages";
import { getLatestVersionRecord } from "@/lib/db/versions";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { PackageWithLatest } from "@/types/api";

/**
 * A2 GET /api/packages/:id — see `docs/api-spec.md` §A2.
 *
 * Returns the single `Package` row plus the latest-version enrichment
 * fields (`latest_released`, `latest_has_repository`, `latest_github_owner`,
 * `latest_github_repo`). Does NOT aggregate A3/A4/A5 counts — callers that
 * need those should hit the list endpoints.
 */
const querySchema = z.object({
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
  const { ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const pkg = await getPackageById(client, { id: idResult.data, ecosystem });
    if (!pkg) return jsonError(404, "Package not found");

    const latest = await getLatestVersionRecord(client, {
      packageId: pkg.id,
      ecosystem,
      latestVersion: pkg.latest_version,
    });

    const body: PackageWithLatest = {
      ...pkg,
      latest_released: latest?.released ?? null,
      latest_has_repository: latest?.has_repository ?? null,
      latest_github_owner: latest?.github_owner ?? null,
      latest_github_repo: latest?.github_repo ?? null,
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
