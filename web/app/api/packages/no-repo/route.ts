import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/no-repo
export async function GET(_req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, v.version
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      WHERE COALESCE(LOWER(v.has_repository::text), '') IN ('false', '0', 'no', '');
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
