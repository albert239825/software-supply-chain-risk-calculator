import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';
import { invalidPathIdMessage, normalizePathId } from '@/lib/api/validation';

// GET /api/packages/:packageId
// A2: Single-package metadata + latest-version enrichment fields.
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
    const result = await pool.query(
      `
        SELECT
          p.id,
          p.ecosystem,
          p.name,
          p.description,
          p.latest_version,
          v.released       AS latest_released,
          v.has_repository AS has_repository,
          v.github_owner   AS github_owner,
          v.github_repo    AS github_repo
        FROM packages p
        LEFT JOIN versions v
          ON v.package_id = p.id AND v.version = p.latest_version
        WHERE p.id = $1
        LIMIT 1;
      `,
      [packageId],
    );
    if (result.rows.length === 0) {
      return new Response(JSON.stringify({ error: 'package not found' }), { status: 404 });
    }
    return new Response(JSON.stringify(result.rows[0]), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
