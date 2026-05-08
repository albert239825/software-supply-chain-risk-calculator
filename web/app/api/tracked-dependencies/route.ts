import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getCurrentUser } from '@/lib/auth';
import {
  invalidPathIdMessage,
  normalizePathId,
  readJsonObject,
} from '@/lib/api/validation';
import { computeComposite, type RiskSignalRanges } from '@/lib/risk/score';

type TrackBody = {
  packageId?: unknown;
  note?: unknown;
};

function normalizeNote(note: unknown): string | null {
  if (typeof note !== 'string') {
    return null;
  }

  const trimmed = note.trim();
  return trimmed.length > 0 ? trimmed.slice(0, 500) : null;
}

export async function GET(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const { rows } = await pool.query<{
      id: string;
      package_id: string;
      note: string | null;
      created_at: string;
      updated_at: string;
      package_name: string;
      ecosystem: string;
      description: string | null;
      latest_version: string;
      latest_version_id: string | null;
      last_release: string | null;
      has_repository: boolean | null;
      maintainer_count: string | number;
      fanout_direct: string | number;
      fanin_dependents: string | number;
    }>(
      `
      WITH latest AS (
        SELECT
          p.id AS package_id,
          v.id AS latest_version_id,
          v.released AS last_release,
          v.has_repository AS has_repository
        FROM packages p
        LEFT JOIN versions v
          ON v.package_id = p.id
          AND v.version = p.latest_version
      ),
      maint AS (
        SELECT
          package_id,
          COUNT(DISTINCT id)::bigint AS maintainer_count
        FROM maintainers
        GROUP BY package_id
      ),
      fanout AS (
        SELECT
          from_version_id,
          COUNT(DISTINCT to_package_id)::bigint AS fanout_direct
        FROM dependencies
        GROUP BY from_version_id
      ),
      fanin AS (
        SELECT
          to_package_id,
          COUNT(DISTINCT from_version_id)::bigint AS fanin_dependents
        FROM dependencies
        GROUP BY to_package_id
      )
      SELECT
        utd.id,
        utd.package_id,
        utd.note,
        utd.created_at,
        utd.updated_at,
        p.name AS package_name,
        p.ecosystem,
        p.description,
        p.latest_version,
        latest.latest_version_id,
        latest.last_release,
        latest.has_repository,
        COALESCE(maint.maintainer_count, 0)::text AS maintainer_count,
        COALESCE(fanout.fanout_direct, 0)::text AS fanout_direct,
        COALESCE(fanin.fanin_dependents, 0)::text AS fanin_dependents
      FROM user_tracked_dependencies utd
      JOIN packages p ON p.id = utd.package_id
      LEFT JOIN latest ON latest.package_id = p.id
      LEFT JOIN maint ON maint.package_id = p.id
      LEFT JOIN fanout ON fanout.from_version_id = latest.latest_version_id
      LEFT JOIN fanin ON fanin.to_package_id = p.id
      WHERE utd.user_id = $1
      ORDER BY utd.created_at DESC;
      `,
      [user.id],
    );

    if (rows.some((row) => typeof (row as Record<string, unknown>).package_id !== 'string')) {
      return NextResponse.json(rows, { status: 200 });
    }

    const now = Date.now();
    const yearsSince = (iso: string | null): number =>
      iso
        ? Math.max(0, (now - new Date(iso).getTime()) / (365.25 * 24 * 3600 * 1000))
        : 0;

    const signalRows = rows.map((row) => ({
      row,
      maintainerCount: Number(row.maintainer_count) || 0,
      stalenessYears: yearsSince(row.last_release),
      fanoutDirect: Number(row.fanout_direct) || 0,
      faninDependents: Number(row.fanin_dependents) || 0,
      hasRepository: Boolean(row.has_repository),
    }));

    const rangeFor = (pick: (signal: (typeof signalRows)[number]) => number) => {
      let min = Infinity;
      let max = -Infinity;
      for (const signal of signalRows) {
        const value = pick(signal);
        if (value < min) min = value;
        if (value > max) max = value;
      }
      if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 0 };
      return { min, max };
    };

    const ranges: RiskSignalRanges = {
      maintainerCount: rangeFor((signal) => signal.maintainerCount),
      stalenessYears: rangeFor((signal) => signal.stalenessYears),
      fanoutDirect: rangeFor((signal) => signal.fanoutDirect),
      faninDependents: rangeFor((signal) => signal.faninDependents),
    };

    const enriched = signalRows.map((signal) => {
      const risk = computeComposite(
        {
          maintainerCount: signal.maintainerCount,
          stalenessYears: signal.stalenessYears,
          fanoutDirect: signal.fanoutDirect,
          faninDependents: signal.faninDependents,
          hasRepository: signal.hasRepository,
        },
        ranges,
      );

      return {
        ...signal.row,
        maintainer_count: signal.maintainerCount,
        fanout_direct: signal.fanoutDirect,
        fanin_dependents: signal.faninDependents,
        has_repository: signal.hasRepository,
        staleness_years: signal.stalenessYears,
        risk_score: risk.composite,
        risk_bucket: risk.bucket,
        checked_at: new Date(now).toISOString(),
      };
    });

    return NextResponse.json(enriched, { status: 200 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}

export async function DELETE(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const result = await pool.query(
      `DELETE FROM user_tracked_dependencies WHERE user_id = $1`,
      [user.id],
    );

    const deleted = result.rowCount ?? 0;
    return NextResponse.json({ deleted }, { status: 200 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}

export async function POST(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const parsed = await readJsonObject(req);
    if (!parsed.ok) {
      return NextResponse.json({ error: parsed.error }, { status: 400 });
    }

    const body = parsed.data as TrackBody;
    const packageId = normalizePathId(body.packageId);
    if (!packageId) {
      return NextResponse.json({ error: invalidPathIdMessage('packageId') }, { status: 400 });
    }

    const note = normalizeNote(body.note);
    const { rows } = await pool.query(
      `
      INSERT INTO user_tracked_dependencies (user_id, package_id, note)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_id, package_id)
      DO UPDATE SET
        note = EXCLUDED.note,
        updated_at = now()
      RETURNING id, package_id, note, created_at, updated_at;
      `,
      [user.id, packageId, note],
    );

    return NextResponse.json(rows[0], { status: 201 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
