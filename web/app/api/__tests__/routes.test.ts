/**
 * Minimal unit tests for every App Router handler under `/api/*`.
 *
 * Strategy: mock `@/lib/db` so every route sees a single spied `pool.query`.
 * For each route we assert:
 *   - happy path: route calls pool.query and returns a 200 JSON body of the
 *     expected shape (usually the mocked rows, verbatim).
 *   - failure path: pool.query throws → route returns 500 with { error }.
 *
 * Tests do NOT talk to a real Postgres. They're intentionally shape-only and
 * catch the common regressions: broken import paths, param-signature drift,
 * lost error handler, response wrapping changes.
 */

import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';

const query: Mock = vi.fn();
vi.mock('@/lib/db', () => ({ default: { query } }));

// Dynamically import route handlers AFTER the mock so they receive the stub.
async function loadGet(path: string): Promise<(req: Request, ctx?: unknown) => Promise<Response>> {
  const mod = (await import(path)) as { GET: (req: Request, ctx?: unknown) => Promise<Response> };
  return mod.GET;
}

function req(url = 'http://localhost/api/test'): Request {
  return new Request(url);
}

function ctx(packageId: string) {
  return { params: Promise.resolve({ packageId }) };
}

async function readJson(res: Response): Promise<unknown> {
  return JSON.parse(await res.text());
}

beforeEach(() => {
  query.mockReset();
});

describe('GET /api/packages/:packageId (A2)', () => {
  it('returns a single package row', async () => {
    const row = { id: 'p1', ecosystem: 'npm', name: 'lodash', latest_version: '4.17.21' };
    query.mockResolvedValueOnce({ rows: [row] });
    const GET = await loadGet('@/app/api/packages/[packageId]/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(row);
  });

  it('returns 404 when the package is unknown', async () => {
    query.mockResolvedValueOnce({ rows: [] });
    const GET = await loadGet('@/app/api/packages/[packageId]/route');
    const res = await GET(req(), ctx('missing'));
    expect(res.status).toBe(404);
    expect(await readJson(res)).toEqual({ error: 'package not found' });
  });

  it('returns 500 on db error', async () => {
    query.mockRejectedValueOnce(new Error('boom'));
    const GET = await loadGet('@/app/api/packages/[packageId]/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(500);
    expect(await readJson(res)).toEqual({ error: 'boom' });
  });
});

describe('GET /api/packages/:packageId/maintainers (A3)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ id: 'm1', username: 'alice' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/[packageId]/maintainers/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/packages/:packageId/dependencies (A4)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_id: 'p2', package_name: 'foo', version_spec: '^1.0.0', dep_kind: 'runtime' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/[packageId]/dependencies/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/packages/:packageId/dependents (A5)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_id: 'p9', package_name: 'consumer', dependent_version: '1.0.0' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/[packageId]/dependents/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/packages/:packageId/versions (R1)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'lodash', version: '4.17.21', released: '2021-01-01' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/[packageId]/versions/route');
    const res = await GET(req(), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/packages/:packageId/graph (R3)', () => {
  it('defaults maxDepth to 4 and returns edges', async () => {
    const rows = [{ from_version_id: 'v1', to_package_id: 'p2', depth: 1 }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/[packageId]/graph/route');
    const res = await GET(req('http://localhost/api/packages/p1/graph'), ctx('p1'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
    expect(query).toHaveBeenCalledWith(expect.any(String), ['p1', 4]);
  });

  it('honours ?maxDepth=', async () => {
    query.mockResolvedValueOnce({ rows: [] });
    const GET = await loadGet('@/app/api/packages/[packageId]/graph/route');
    await GET(req('http://localhost/api/packages/p1/graph?maxDepth=2'), ctx('p1'));
    expect(query).toHaveBeenCalledWith(expect.any(String), ['p1', 2]);
  });
});

describe('GET /api/packages/all', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'react', version: '19.0.0', released: '2024-12-05' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/all/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/packages/no-repo (R9)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'mystery', version: '1.0.0' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/packages/no-repo/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/stats/top-fanout (R2)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'webpack', num_dependencies: '42' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/stats/top-fanout/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/stats/most-dependents (R5)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'lodash', dependents: '9000' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/stats/most-dependents/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/stats/depth-below (R8)', () => {
  it('defaults n to the route default and returns rows', async () => {
    const rows = [{ package_name: 'tiny', max_dependency_depth: 1 }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/stats/depth-below/route');
    const res = await GET(req('http://localhost/api/stats/depth-below?n=2'));
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
    // SQL and the numeric bind arg must be passed through.
    const call = query.mock.calls[0];
    expect(typeof call[0]).toBe('string');
    expect(call[1]).toEqual([2]);
  });
});

describe('GET /api/maintainers/top (R6)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ username: 'sindresorhus', num_packages: '1000' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/maintainers/top/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/risk/abandoned-popular (R7)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'old', dependents: 100, last_release: '2019-01-01' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/risk/abandoned-popular/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/risk/stale-low-maintainer (R4)', () => {
  it('returns rows verbatim', async () => {
    const rows = [{ package_name: 'solo', maintainer_count: '1', last_release: '2020-01-01' }];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/risk/stale-low-maintainer/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual(rows);
  });
});

describe('GET /api/risk/ranked (R10 — TS-scored)', () => {
  it('returns [] when no packages', async () => {
    query.mockResolvedValueOnce({ rows: [] });
    const GET = await loadGet('@/app/api/risk/ranked/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    expect(await readJson(res)).toEqual([]);
  });

  it('computes composite in TS and sorts desc by risk_score', async () => {
    // Two packages with different signals. The "bad" one should rank first.
    const rows = [
      {
        package_id: 'good',
        package_name: 'healthy',
        maintainer_count: 10,
        fanout_direct: 1,
        fanin_dependents: 2,
        last_release: new Date().toISOString(),
        has_repository: true,
      },
      {
        package_id: 'bad',
        package_name: 'risky',
        maintainer_count: 1,
        fanout_direct: 20,
        fanin_dependents: 200,
        last_release: '2018-01-01T00:00:00Z',
        has_repository: false,
      },
    ];
    query.mockResolvedValueOnce({ rows });
    const GET = await loadGet('@/app/api/risk/ranked/route');
    const res = await GET(req());
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as Array<{
      package_id: string;
      risk_score: number;
      bucket: string;
    }>;
    expect(body.map((r) => r.package_id)).toEqual(['bad', 'good']);
    for (const row of body) {
      expect(row.risk_score).toBeGreaterThanOrEqual(0);
      expect(row.risk_score).toBeLessThanOrEqual(1);
      expect(['low', 'medium', 'high']).toContain(row.bucket);
    }
  });

  it('returns 500 on db error', async () => {
    query.mockRejectedValueOnce(new Error('db down'));
    const GET = await loadGet('@/app/api/risk/ranked/route');
    const res = await GET(req());
    expect(res.status).toBe(500);
    expect(await readJson(res)).toEqual({ error: 'db down' });
  });
});
