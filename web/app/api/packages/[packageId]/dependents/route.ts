import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/:packageId/dependents
// A5: Packages that depend (directly) on :packageId via any of their versions.
export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ packageId: string }> },
) {
  const { packageId } = await params;
  try {
    const result = await pool.query(
      `
        SELECT DISTINCT
          dep_pkg.id   AS package_id,
          dep_pkg.name AS package_name,
          v.version    AS dependent_version
        FROM dependencies d
        JOIN versions v      ON v.id = d.from_version_id
        JOIN packages dep_pkg ON dep_pkg.id = v.package_id
        WHERE d.to_package_id = $1
        ORDER BY dep_pkg.name, v.version;
      `,
      [packageId],
    );
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
