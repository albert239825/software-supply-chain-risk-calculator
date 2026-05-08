import pool from '../../../../../lib/db';
import { NextRequest } from 'next/server';
import {
  invalidPathIdMessage,
  normalizePathId,
  parseBoundedIntegerParam,
} from '@/lib/api/validation';

// GET /api/packages/:packageId/graph?maxDepth=4&maxOrder=2
// :packageId is the root **versions.id**. maxOrder caps outbound hop depth shown (1=direct deps only).
export async function GET(req: NextRequest, context: { params: Promise<{ packageId: string }> }) {
  const routeParams = await context.params;
  const packageId = normalizePathId(routeParams.packageId);
  if (!packageId) {
    return new Response(JSON.stringify({ error: invalidPathIdMessage('packageId') }), {
      status: 400,
    });
  }

  const { searchParams } = new URL(req.url);

  const maxDepthParam = parseBoundedIntegerParam(searchParams, 'maxDepth', 4, 1, 32);
  if (!maxDepthParam.ok) {
    return new Response(JSON.stringify({ error: maxDepthParam.error }), { status: 400 });
  }

  const maxOrderParam = parseBoundedIntegerParam(searchParams, 'maxOrder', 2, 1, 4);
  if (!maxOrderParam.ok) {
    return new Response(JSON.stringify({ error: maxOrderParam.error }), { status: 400 });
  }

  const maxDepth = maxDepthParam.value;
  const maxOrder = maxOrderParam.value;

  try {
    const result = await pool.query(
      `
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
         WHERE b.depth < $2::int
      ),
      dep_tree AS (
        SELECT
          pkg_from.id::text          AS from_package_id,
          d.from_version_id,
          pkg_to.id::text            AS to_package_id,
          pkg_from.name              AS from_package,
          d.from_version             AS from_version,
          pkg_to.name                AS to_package,
          d.version_spec,
          d.dep_kind,
          b.depth + 1               AS depth
        FROM bfs b
        JOIN LATERAL unnest(b.frontier) AS cur(from_version_id) ON TRUE
        JOIN dependencies d ON d.from_version_id::text = cur.from_version_id
        JOIN versions vf ON vf.id::text = d.from_version_id::text
        JOIN packages pkg_from ON pkg_from.id = vf.package_id
        JOIN packages pkg_to ON pkg_to.id::text = d.to_package_id::text
      ),
      graph AS (
         SELECT DISTINCT ON (from_package_id, to_package_id, dep_kind)
            from_package_id,
            from_version_id,
            to_package_id,
            from_package,
            from_version,
            to_package,
            version_spec,
            dep_kind,
            depth
         FROM dep_tree
         ORDER BY from_package_id, to_package_id, dep_kind, depth ASC
      )
      SELECT *
      FROM graph
      WHERE depth <= $3::int;
    `,
      [packageId, maxDepth, maxOrder],
    );
    return new Response(JSON.stringify(result.rows), { status: 200 });
  } catch (error) {
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
}
