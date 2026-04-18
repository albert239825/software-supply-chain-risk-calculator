import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/risk/ranked
export async function GET(req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name,
        COUNT(DISTINCT m.id) AS maintainers,
        COUNT(DISTINCT d.to_package_id) AS dependencies,
        MAX(v.released) AS last_release,
        (
            COUNT(DISTINCT d.to_package_id) * 0.4 +
            (2 - COUNT(DISTINCT m.id)) * 0.3 +
            EXTRACT(YEAR FROM AGE(NOW(), MAX(v.released)::timestamptz)) * 0.3
        ) AS risk_score
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      LEFT JOIN dependencies d ON v.id = d.from_version_id
      LEFT JOIN maintainers m ON p.id = m.package_id
      GROUP BY p.name
      ORDER BY risk_score DESC
      LIMIT 20;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
