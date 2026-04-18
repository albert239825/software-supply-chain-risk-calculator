import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/risk/stale-low-maintainer
export async function GET(req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, COUNT(m.id) AS maintainer_count, MAX(v.released) AS last_release
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      LEFT JOIN maintainers m ON p.id = m.package_id
      GROUP BY p.name
      HAVING COUNT(m.id) <= 2
      ORDER BY last_release ASC;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
