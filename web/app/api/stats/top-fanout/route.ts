import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/stats/top-fanout
export async function GET(_req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, COUNT(d.to_package_id) AS num_dependencies
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      JOIN dependencies d ON v.id = d.from_version_id
      GROUP BY p.name
      ORDER BY num_dependencies DESC
      LIMIT 10;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
