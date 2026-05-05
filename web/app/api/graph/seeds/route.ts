import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

/** GET /api/graph/seeds — popular dependency roots for the Graph Explorer UI. */

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const raw = parseInt(searchParams.get('limit') || '16', 10);
  const limit = Number.isFinite(raw) ? Math.min(Math.max(raw, 1), 48) : 16;

  try {
    const result = await pool.query<{
      package_id: string;
      version_id: string;
      ecosystem: string;
      package_name: string;
      version: string;
      dependency_count: string | number;
    }>(
      `
      SELECT
        p.id AS package_id,
        v.id AS version_id,
        p.ecosystem,
        p.name AS package_name,
        v.version,
        COUNT(*)::bigint AS dependency_count
      FROM dependencies d
      JOIN versions v ON v.id = d.from_version_id
      JOIN packages p ON p.id = v.package_id
      GROUP BY p.id, v.id, p.ecosystem, p.name, v.version
      ORDER BY COUNT(*) DESC NULLS LAST, p.name ASC, v.version ASC
      LIMIT $1;
      `,
      [limit],
    );

    const rows = result.rows.map((r) => ({
      package_id: r.package_id,
      version_id: r.version_id,
      ecosystem: r.ecosystem,
      package_name: r.package_name,
      version: r.version,
      dependency_count: Number(r.dependency_count) || 0,
    }));

    return new Response(JSON.stringify(rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
