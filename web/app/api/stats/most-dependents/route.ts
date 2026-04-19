import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/stats/most-dependents
export async function GET(_req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, COUNT(*) AS dependents
      FROM dependencies d
      JOIN packages p ON d.to_package_id = p.id
      GROUP BY p.name
      ORDER BY dependents DESC
      LIMIT 10;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
