import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import { countDependencies } from "@/lib/db/dependencies";
import { countMaintainers } from "@/lib/db/maintainers";
import { countPackages } from "@/lib/db/packages";
import { countVersions } from "@/lib/db/versions";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { Ecosystem } from "@/types/api";

/**
 * A7 GET /api/stats/counts — see `docs/api-spec.md` §A7.
 *
 * Returns global counts for the Home-page header card. The four count
 * queries are issued in parallel via `Promise.all` since they hit
 * independent tables.
 */
const countsQuerySchema = z.object({
  ecosystem: ecosystemSchema,
});

type CountsResponse = {
  packages: number;
  versions: number;
  maintainers: number;
  dependencies: number;
  ecosystem: Ecosystem;
};

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), countsQuerySchema);
  if (parsed.error) return parsed.error;
  const { ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const [packages, versions, maintainers, dependencies] = await Promise.all([
      countPackages(client, { ecosystem }),
      countVersions(client, { ecosystem }),
      countMaintainers(client, { ecosystem }),
      countDependencies(client, { ecosystem }),
    ]);
    const body: CountsResponse = {
      packages,
      versions,
      maintainers,
      dependencies,
      ecosystem,
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
