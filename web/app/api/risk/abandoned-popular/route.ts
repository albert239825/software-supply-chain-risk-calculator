import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/risk/abandoned-popular
export async function GET(req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, COUNT(DISTINCT d.from_package) AS dependents, MAX(v.released) AS last_release
      FROM packages p
      JOIN versions v ON v.package_id = p.id
      JOIN dependencies d ON d.to_package_id = p.id
      GROUP BY p.id, p.name
      HAVING MAX(v.released::timestamp) < (NOW() - INTERVAL '2 years')
      ORDER BY dependents DESC;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
