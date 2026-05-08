import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';
import { invalidPathIdMessage, normalizePathId } from '@/lib/api/validation';

// GET /api/packages/:packageId/versions
export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ packageId: string }> },
) {
  const routeParams = await params;
  const packageId = normalizePathId(routeParams.packageId);
  if (!packageId) {
    return new Response(JSON.stringify({ error: invalidPathIdMessage('packageId') }), {
      status: 400,
    });
  }

  try {
    const result = await pool.query(`
      SELECT p.name AS package_name, v.version, v.released
      FROM packages p
      JOIN versions v ON p.id = v.package_id
      WHERE p.id = $1
      ORDER BY v.released DESC;
    `, [packageId]);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
