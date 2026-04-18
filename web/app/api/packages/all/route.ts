import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/all
export async function GET(req: NextRequest) {
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, v.version, v.released
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      ORDER BY v.released DESC;
    `);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: error.message }), { status: 500 });
  }
}
