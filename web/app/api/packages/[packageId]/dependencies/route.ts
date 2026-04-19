import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/:packageId/dependencies
// A4: Direct dependencies of the latest version of :packageId.
export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ packageId: string }> },
) {
  const { packageId } = await params;
  try {
    const result = await pool.query(
      `
        SELECT DISTINCT
          dep_pkg.id    AS package_id,
          dep_pkg.name  AS package_name,
          d.version_spec,
          d.dep_kind
        FROM packages p
        JOIN versions  v       ON v.package_id = p.id AND v.version = p.latest_version
        JOIN dependencies d    ON d.from_version_id = v.id
        LEFT JOIN packages dep_pkg ON dep_pkg.id = d.to_package_id
        WHERE p.id = $1
        ORDER BY package_name NULLS LAST;
      `,
      [packageId],
    );
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
