import pool from '../../../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/:packageId/versions
export async function GET(req: NextRequest, { params }: { params: { packageId: string } }) {
  const { packageId } = params;
  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, v.version, v.released
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      WHERE p.id = $1
      ORDER BY v.released DESC;
    `, [packageId]);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
