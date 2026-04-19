import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/:packageId/maintainers
// A3: Maintainers of a given package.
export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ packageId: string }> },
) {
  const { packageId } = await params;
  try {
    const result = await pool.query(
      `
        SELECT m.id, m.username, m.name, m.role, m.email
        FROM maintainers m
        WHERE m.package_id = $1
        ORDER BY m.username;
      `,
      [packageId],
    );
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
