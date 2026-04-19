import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';
import { computeComposite, type RiskSignalRanges } from '../../../../lib/risk/score';

// GET /api/risk/ranked
// R10: Composite risk score per package, ranked desc. The composite formula
// lives in `web/lib/risk/score.ts` (shared with future A6) — do NOT inline
// weights in SQL. SQL only materializes the per-package raw signals; the
// score is computed in TS so A6 and R10 stay in lockstep.
export async function GET(_req: NextRequest) {
  try {
    const { rows } = await pool.query<{
      package_id: string;
      package_name: string;
      maintainer_count: string | number;
      fanout_direct: string | number;
      fanin_dependents: string | number;
      last_release: string | null;
      has_repository: boolean | null;
    }>(`
      WITH latest AS (
        SELECT p.id AS package_id, v.id AS version_id,
               v.released AS last_release,
               v.has_repository AS has_repository
        FROM   packages p
        LEFT JOIN versions v
               ON v.package_id = p.id AND v.version = p.latest_version
      )
      SELECT
        p.id   AS package_id,
        p.name AS package_name,
        (SELECT COUNT(DISTINCT m.id)
           FROM maintainers m WHERE m.package_id = p.id)            AS maintainer_count,
        (SELECT COUNT(DISTINCT d.to_package_id)
           FROM dependencies d
           WHERE d.from_version_id = latest.version_id)             AS fanout_direct,
        (SELECT COUNT(DISTINCT d.from_version_id)
           FROM dependencies d WHERE d.to_package_id = p.id)        AS fanin_dependents,
        latest.last_release                                         AS last_release,
        latest.has_repository                                       AS has_repository
      FROM packages p
      JOIN latest ON latest.package_id = p.id;
    `);

    const now = Date.now();
    const yearsSince = (iso: string | null): number =>
      iso ? Math.max(0, (now - new Date(iso).getTime()) / (365.25 * 24 * 3600 * 1000)) : 0;

    const signals = rows.map((r) => ({
      package_id: r.package_id,
      package_name: r.package_name,
      maintainerCount: Number(r.maintainer_count) || 0,
      stalenessYears: yearsSince(r.last_release),
      fanoutDirect: Number(r.fanout_direct) || 0,
      faninDependents: Number(r.fanin_dependents) || 0,
      hasRepository: Boolean(r.has_repository),
      last_release: r.last_release,
    }));

    if (signals.length === 0) {
      return new Response(JSON.stringify([]), { status: 200 });
    }

    const rangeFor = (pick: (s: (typeof signals)[number]) => number) => {
      let min = Infinity;
      let max = -Infinity;
      for (const s of signals) {
        const v = pick(s);
        if (v < min) min = v;
        if (v > max) max = v;
      }
      if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 0 };
      return { min, max };
    };

    const ranges: RiskSignalRanges = {
      maintainerCount: rangeFor((s) => s.maintainerCount),
      stalenessYears: rangeFor((s) => s.stalenessYears),
      fanoutDirect: rangeFor((s) => s.fanoutDirect),
      faninDependents: rangeFor((s) => s.faninDependents),
    };

    const scored = signals.map((s) => {
      const breakdown = computeComposite(
        {
          maintainerCount: s.maintainerCount,
          stalenessYears: s.stalenessYears,
          fanoutDirect: s.fanoutDirect,
          faninDependents: s.faninDependents,
          hasRepository: s.hasRepository,
        },
        ranges,
      );
      return {
        package_id: s.package_id,
        package_name: s.package_name,
        maintainers: s.maintainerCount,
        dependencies: s.fanoutDirect,
        dependents: s.faninDependents,
        last_release: s.last_release,
        risk_score: breakdown.composite,
        bucket: breakdown.bucket,
      };
    });

    scored.sort((a, b) => b.risk_score - a.risk_score);
    return new Response(JSON.stringify(scored.slice(0, 20)), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
