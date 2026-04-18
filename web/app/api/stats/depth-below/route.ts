import pool from '../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/stats/depth-below?n=3
export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const n = parseInt(searchParams.get('n') || '3', 10);
  try {
    const result = await pool.query(`
      WITH RECURSIVE resolved_edges AS (
          SELECT DISTINCT d.from_version_id AS from_version_id, v.id AS to_version_id
          FROM dependencies d
          JOIN packages p_to ON p_to.id = d.to_package_id
          JOIN versions v ON v.package_id = p_to.id AND v.version = p_to.latest_version
          WHERE d.from_version_id IS NOT NULL AND d.to_package_id IS NOT NULL
      ),
      walk AS (
          SELECT p_from.id AS root_package_id, p_from.name AS root_package, e.to_version_id AS current_version_id, 1 AS depth
          FROM resolved_edges e
          JOIN versions v_from ON v_from.id = e.from_version_id
          JOIN packages p_from ON p_from.id = v_from.package_id
          UNION ALL
          SELECT w.root_package_id, w.root_package, e.to_version_id AS current_version_id, w.depth + 1 AS depth
          FROM walk w
          JOIN resolved_edges e ON e.from_version_id = w.current_version_id
          WHERE w.depth < $1
      ),
      package_depths AS (
          SELECT p.id AS package_id, p.name AS package_name, COALESCE(MAX(w.depth), 0) AS max_dependency_depth
          FROM packages p
          LEFT JOIN walk w ON w.root_package_id = p.id
          GROUP BY p.id, p.name
      )
      SELECT package_id, package_name, max_dependency_depth
      FROM package_depths
      WHERE max_dependency_depth < $1
      ORDER BY max_dependency_depth, package_name;
    `, [n]);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
