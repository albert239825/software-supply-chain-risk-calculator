import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

/** GET /api/graph/search?q= — autocomplete packages for Graph Explorer */

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get('q')?.trim().slice(0, 96) ?? '';
  if (q.length < 2) {
    return new Response(JSON.stringify([]), { status: 200 });
  }

  try {
    const { rows } = await pool.query<{
      package_id: string;
      package_name: string;
      ecosystem: string;
      latest_version: string;
      latest_version_id: string | null;
    }>(
      `
      SELECT
        p.id                           AS package_id,
        p.name                         AS package_name,
        p.ecosystem                   AS ecosystem,
        p.latest_version              AS latest_version,
        (
          SELECT v.id
          FROM versions v
          WHERE v.package_id = p.id
            AND v.version = p.latest_version
          LIMIT 1
        )                             AS latest_version_id
      FROM packages p
      WHERE strpos(lower(p.name), lower($1)) > 0
      ORDER BY length(p.name) ASC, p.name ASC
      LIMIT 20;
      `,
      [q],
    );

    return new Response(JSON.stringify(rows.filter((r) => r.latest_version_id)), {
      status: 200,
    });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
