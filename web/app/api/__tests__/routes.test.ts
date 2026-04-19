/**
 * Integration tests for every App Router handler under `/api/*`.
 *
 * These hit REAL Supabase via the same `pg.Pool` the routes use. They are
 * read-only and make shape-only assertions so they stay green even as the
 * underlying data set grows or shifts. They do NOT cover write paths (we
 * don't have any).
 *
 * Running locally:
 *   - put SUPABASE_DB_URL into web/.env.local (same value the Next dev
 *     server uses)
 *   - `cd web && npm test`
 *
 * Without that env var the entire suite skips (see `skip` below) so other
 * developers / CI without the secret still get a green unit-test run.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pool from '@/lib/db';

// Loaded handlers, filled in by beforeAll().
type Handler = (req: Request, ctx?: unknown) => Promise<Response>;
const h: Record<string, Handler> = {};

// A real package id discovered from the DB at suite start. Needed by every
// /api/packages/[packageId]/* route.
let samplePackageId = '';

const skip = !process.env.SUPABASE_DB_URL;

function req(url = 'http://localhost/api/test'): Request {
  return new Request(url);
}

function ctx(packageId: string) {
  return { params: Promise.resolve({ packageId }) };
}

async function readJson(res: Response): Promise<unknown> {
  return JSON.parse(await res.text());
}

async function load(path: string): Promise<Handler> {
  const mod = (await import(path)) as { GET: Handler };
  return mod.GET;
}

describe.skipIf(skip)('integration: /api/*', () => {
  beforeAll(async () => {
    // Fail fast if the pool can't connect — better error than 15 timeouts.
    await pool.query('SELECT 1');

    const { rows } = await pool.query<{ id: string }>(
      'SELECT id FROM packages LIMIT 1',
    );
    if (rows.length === 0) {
      throw new Error('packages table is empty; seed the DB before running integration tests');
    }
    samplePackageId = rows[0].id;

    // Load every route handler once so individual tests stay tight.
    h.a2 = await load('@/app/api/packages/[packageId]/route');
    h.a3 = await load('@/app/api/packages/[packageId]/maintainers/route');
    h.a4 = await load('@/app/api/packages/[packageId]/dependencies/route');
    h.a5 = await load('@/app/api/packages/[packageId]/dependents/route');
    h.r1 = await load('@/app/api/packages/[packageId]/versions/route');
    h.r3 = await load('@/app/api/packages/[packageId]/graph/route');
    h.all = await load('@/app/api/packages/all/route');
    h.r9 = await load('@/app/api/packages/no-repo/route');
    h.r2 = await load('@/app/api/stats/top-fanout/route');
    h.r5 = await load('@/app/api/stats/most-dependents/route');
    h.r8 = await load('@/app/api/stats/depth-below/route');
    h.r6 = await load('@/app/api/maintainers/top/route');
    h.r7 = await load('@/app/api/risk/abandoned-popular/route');
    h.r4 = await load('@/app/api/risk/stale-low-maintainer/route');
    h.r10 = await load('@/app/api/risk/ranked/route');
  });

  afterAll(async () => {
    // Stop vitest from hanging on the open pool.
    await pool.end();
  });

  it('A2 /api/packages/:packageId → 200 with a package row', async () => {
    const res = await h.a2(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as Record<string, unknown>;
    expect(body).toMatchObject({
      id: samplePackageId,
      name: expect.any(String),
      ecosystem: expect.any(String),
    });
  });

  it('A2 /api/packages/:packageId → 404 for unknown id', async () => {
    const res = await h.a2(req(), ctx('00000000-0000-0000-0000-000000000000'));
    expect(res.status).toBe(404);
    expect(await readJson(res)).toEqual({ error: 'package not found' });
  });

  it('A3 /api/packages/:packageId/maintainers → 200 array', async () => {
    const res = await h.a3(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('A4 /api/packages/:packageId/dependencies → 200 array', async () => {
    const res = await h.a4(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('A5 /api/packages/:packageId/dependents → 200 array', async () => {
    const res = await h.a5(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R1 /api/packages/:packageId/versions → 200 array', async () => {
    const res = await h.r1(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R3 /api/packages/:packageId/graph → 200 array (default maxDepth=4)', async () => {
    const res = await h.r3(
      req(`http://localhost/api/packages/${samplePackageId}/graph`),
      ctx(samplePackageId),
    );
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R3 /api/packages/:packageId/graph?maxDepth=1 → 200 array', async () => {
    const res = await h.r3(
      req(`http://localhost/api/packages/${samplePackageId}/graph?maxDepth=1`),
      ctx(samplePackageId),
    );
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('/api/packages/all → 200 non-empty array', async () => {
    const res = await h.all(req());
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as unknown[];
    expect(Array.isArray(body)).toBe(true);
    expect(body.length).toBeGreaterThan(0);
  });

  it('R9 /api/packages/no-repo → 200 array', async () => {
    const res = await h.r9(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R2 /api/stats/top-fanout → 200 array', async () => {
    const res = await h.r2(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R5 /api/stats/most-dependents → 200 array', async () => {
    const res = await h.r5(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R8 /api/stats/depth-below?n=2 → 200 array', async () => {
    const res = await h.r8(req('http://localhost/api/stats/depth-below?n=2'));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R6 /api/maintainers/top → 200 array', async () => {
    const res = await h.r6(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R7 /api/risk/abandoned-popular → 200 array', async () => {
    const res = await h.r7(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  it('R4 /api/risk/stale-low-maintainer → 200 array', async () => {
    const res = await h.r4(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  // R10 scans the full packages table + 3 correlated subqueries per row,
  // so the route itself can take >20s against an unindexed Supabase. Bump
  // the per-test timeout; the SQL will get a LIMIT / materialized-view
  // optimization in a follow-up.
  it('R10 /api/risk/ranked → TS-scored rows with risk_score in [0,1] and valid bucket', { timeout: 90_000 }, async () => {
    const res = await h.r10(req());
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as Array<{
      package_id: string;
      package_name: string;
      risk_score: number;
      bucket: string;
    }>;
    expect(Array.isArray(body)).toBe(true);
    expect(body.length).toBeGreaterThan(0);
    // Sorted desc.
    for (let i = 1; i < body.length; i++) {
      expect(body[i - 1].risk_score).toBeGreaterThanOrEqual(body[i].risk_score);
    }
    for (const row of body) {
      expect(row.risk_score).toBeGreaterThanOrEqual(0);
      expect(row.risk_score).toBeLessThanOrEqual(1);
      expect(['low', 'medium', 'high']).toContain(row.bucket);
      expect(typeof row.package_name).toBe('string');
    }
  });
});

// Also emit a single test in the unskipped path so reporters have something
// to say when the secret is missing.
describe.skipIf(!skip)('integration: /api/* (skipped — SUPABASE_DB_URL not set)', () => {
  it('set SUPABASE_DB_URL in web/.env.local to run integration tests', () => {
    expect(skip).toBe(true);
  });
});
