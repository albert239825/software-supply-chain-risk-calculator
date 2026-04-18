import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/maintainers/top
export async function GET(req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT m.username, COUNT(DISTINCT m.package_id) AS num_packages
      FROM maintainers m
      GROUP BY m.username
      ORDER BY num_packages DESC
      LIMIT 10;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
