import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';

// GET /api/packages/:packageId/graph?maxDepth=4
export async function GET(req: NextRequest, { params }: { params: { packageId: string } }) {
  const { packageId } = params;
  const { searchParams } = new URL(req.url);
  const maxDepth = parseInt(searchParams.get('maxDepth') || '4', 10);
  try {
    const result = await pool.query(`
      WITH RECURSIVE bfs AS (
         SELECT 0 AS depth, ARRAY[$1::text] AS frontier, ARRAY[$1::text] AS seen
         UNION ALL
         SELECT b.depth + 1, nxt.next_frontier, b.seen || nxt.next_frontier
         FROM bfs b
         JOIN LATERAL (
           SELECT array_agg(DISTINCT v_next.id::text) AS next_frontier
           FROM unnest(b.frontier) AS cur(from_version_id)
           JOIN dependencies d ON d.from_version_id::text = cur.from_version_id
           JOIN packages p ON p.id::text = d.to_package_id::text
           JOIN versions v_next ON v_next.package_id::text = p.id::text AND v_next.version = p.latest_version
           WHERE NOT (v_next.id::text = ANY(b.seen))
         ) AS nxt ON cardinality(nxt.next_frontier) > 0
         WHERE b.depth < $2
      ),
      dep_tree AS (
         SELECT d.from_version_id, d.to_package_id, d.from_package, d.from_version, d.to_package, d.version_spec, d.dep_kind, b.depth + 1 AS depth
         FROM bfs b
         JOIN LATERAL unnest(b.frontier) AS cur(from_version_id) ON TRUE
         JOIN dependencies d ON d.from_version_id::text = cur.from_version_id
      ),
      graph AS (
         SELECT DISTINCT ON (to_package_id, dep_kind) from_version_id, to_package_id, from_package, from_version, to_package, version_spec, dep_kind, depth
         FROM dep_tree
         ORDER BY to_package_id, dep_kind, depth
      )
      SELECT max(g.depth) FROM graph g;
    `, [packageId, maxDepth]);
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error: any) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
