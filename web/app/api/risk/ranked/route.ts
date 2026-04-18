import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";

import { ecosystemSchema, jsonError, parseQuery } from "@/lib/api/params";
import {
  fetchRiskRangesForEcosystem,
  fetchRiskSignalsForEcosystem,
} from "@/lib/db/rankings";
import { computeComposite } from "@/lib/risk/score";
import { createSupabaseServerClient } from "@/lib/supabase";
import type { ListResponse, Package } from "@/types/api";

/**
 * R10 GET /api/risk/ranked — see `docs/api-spec.md` §R10 (Q10).
 *
 * Phase 1 implementation:
 *   1. Fetch all per-package signal rows for the ecosystem from the
 *      `v_risk_signals` view (see `lib/db/rankings.ts` header for DDL).
 *   2. Fetch ecosystem-wide signal ranges for min-max normalization.
 *   3. Run the shared `computeComposite` TS library on each row so R10 and
 *      the forthcoming A6 (per-package breakdown) agree on the score.
 *   4. Sort by composite DESC (stable), slice by `limit`/`offset`.
 *
 * This is O(N) work per request. For larger datasets the next step is a
 * materialized view `mv_risk_ranked` that persists the precomputed
 * composite + bucket — see `docs/proposed-indexes.md` and
 * `docs/query-optimization-log.md` for the optimization plan.
 *
 * R10 deliberately returns only `{ package, composite, bucket }`; the
 * per-signal breakdown is A6's job so the ranking payload stays compact.
 */
const rankedQuerySchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(20),
  offset: z.coerce.number().int().min(0).default(0),
  ecosystem: ecosystemSchema,
});

type RankedItem = {
  package: Package;
  composite: number;
  bucket: "low" | "medium" | "high";
};

export async function GET(request: NextRequest): Promise<NextResponse> {
  const parsed = parseQuery(new URL(request.url), rankedQuerySchema);
  if (parsed.error) return parsed.error;
  const { limit, offset, ecosystem } = parsed.data;

  try {
    const client = createSupabaseServerClient();
    const [rows, ranges] = await Promise.all([
      fetchRiskSignalsForEcosystem(client, { ecosystem }),
      fetchRiskRangesForEcosystem(client, { ecosystem }),
    ]);

    const scored: RankedItem[] = rows.map(({ package: pkg, signals }) => {
      const breakdown = computeComposite(signals, ranges);
      return {
        package: pkg,
        composite: breakdown.composite,
        bucket: breakdown.bucket,
      };
    });

    // Stable sort by composite DESC. `Array.prototype.sort` is stable in
    // modern V8 so ties preserve their input order (driven by the view's
    // own row ordering, which is documented as non-deterministic — hence
    // the secondary sort on package name for deterministic output).
    scored.sort((a, b) => {
      if (b.composite !== a.composite) return b.composite - a.composite;
      return a.package.name.localeCompare(b.package.name);
    });

    const total = scored.length;
    const items = scored.slice(offset, offset + limit);
    const body: ListResponse<RankedItem> = {
      items,
      meta: { total, limit, offset },
    };
    return NextResponse.json(body);
  } catch (err) {
    const message = err instanceof Error ? err.message : "unknown error";
    return jsonError(500, message);
  }
}
