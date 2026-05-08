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
 *     server uses), and ensure the DB is reachable from this machine
 *   - `cd web && npm test`
 *
 * If `SUPABASE_DB_URL` is unset, or the database cannot be reached (e.g.
 * offline / DNS / firewall), tests are skipped so CI and sandboxes stay green.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { NextRequest } from "next/server";
import pool from "@/lib/db";

type Handler = (req: Request, ctx?: unknown) => Promise<Response>;
const h: Record<string, Handler> = {};

let samplePackageId = "";

let integrationReady = false;

function req(url = "http://localhost/api/test"): NextRequest {
  return new NextRequest(url);
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

function itDb(
  name: string,
  fn: () => Promise<void>,
  options?: { timeout?: number },
) {
  it(name, options ?? {}, async ({ skip }) => {
    if (!process.env.SUPABASE_DB_URL) {
      skip(true, "SUPABASE_DB_URL not set");
    }
    if (!integrationReady) {
      skip(true, "database unreachable");
    }
    await fn();
  });
}

describe("integration: /api/*", () => {
  beforeAll(async () => {
    if (!process.env.SUPABASE_DB_URL) {
      return;
    }
    try {
      await pool.query("SELECT 1");

      const { rows } = await pool.query<{ id: string }>(
        "SELECT id FROM packages LIMIT 1",
      );
      if (rows.length === 0) {
        throw new Error(
          "packages table is empty; seed the DB before running integration tests",
        );
      }
      samplePackageId = rows[0].id;

      h.a2 = await load("@/app/api/packages/[packageId]/route");
      h.a3 = await load("@/app/api/packages/[packageId]/maintainers/route");
      h.a4 = await load("@/app/api/packages/[packageId]/dependencies/route");
      h.a5 = await load("@/app/api/packages/[packageId]/dependents/route");
      h.r1 = await load("@/app/api/packages/[packageId]/versions/route");
      h.r3 = await load("@/app/api/packages/[packageId]/graph/route");
      h.all = await load("@/app/api/packages/all/route");
      h.r9 = await load("@/app/api/packages/no-repo/route");
      h.r2 = await load("@/app/api/stats/top-fanout/route");
      h.r5 = await load("@/app/api/stats/most-dependents/route");
      h.r8 = await load("@/app/api/stats/depth-below/route");
      h.r6 = await load("@/app/api/maintainers/top/route");
      h.r7 = await load("@/app/api/risk/abandoned-popular/route");
      h.r4 = await load("@/app/api/risk/stale-low-maintainer/route");
      h.r10 = await load("@/app/api/risk/ranked/route");

      integrationReady = true;
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn("[vitest] Skipping API integration tests:", err);
    }
  });

  afterAll(async () => {
    await pool.end().catch(() => undefined);
  });

  itDb("A2 /api/packages/:packageId → 200 with a package row", async () => {
    const res = await h.a2(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as Record<string, unknown>;
    expect(body).toMatchObject({
      id: samplePackageId,
      name: expect.any(String),
      ecosystem: expect.any(String),
    });
  });

  itDb("A2 /api/packages/:packageId → 404 for unknown id", async () => {
    const res = await h.a2(req(), ctx("00000000-0000-0000-0000-000000000000"));
    expect(res.status).toBe(404);
    expect(await readJson(res)).toEqual({ error: "package not found" });
  });

  itDb("A3 /api/packages/:packageId/maintainers → 200 array", async () => {
    const res = await h.a3(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("A4 /api/packages/:packageId/dependencies → 200 array", async () => {
    const res = await h.a4(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("A5 /api/packages/:packageId/dependents → 200 array", async () => {
    const res = await h.a5(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R1 /api/packages/:packageId/versions → 200 array", async () => {
    const res = await h.r1(req(), ctx(samplePackageId));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R3 /api/packages/:packageId/graph → 200 array (default maxDepth=4)", async () => {
    const res = await h.r3(
      req(`http://localhost/api/packages/${samplePackageId}/graph`),
      ctx(samplePackageId),
    );
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R3 /api/packages/:packageId/graph?maxDepth=1 → 200 array", async () => {
    const res = await h.r3(
      req(`http://localhost/api/packages/${samplePackageId}/graph?maxDepth=1`),
      ctx(samplePackageId),
    );
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("/api/packages/all → 200 non-empty array", async () => {
    const res = await h.all(req());
    expect(res.status).toBe(200);
    const body = (await readJson(res)) as unknown[];
    expect(Array.isArray(body)).toBe(true);
    expect(body.length).toBeGreaterThan(0);
  });

  itDb("R9 /api/packages/no-repo → 200 array", async () => {
    const res = await h.r9(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R2 /api/stats/top-fanout → 200 array", async () => {
    const res = await h.r2(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R5 /api/stats/most-dependents → 200 array", async () => {
    const res = await h.r5(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R8 /api/stats/depth-below?n=2 → 200 array", async () => {
    const res = await h.r8(req("http://localhost/api/stats/depth-below?n=2"));
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R6 /api/maintainers/top → 200 array", async () => {
    const res = await h.r6(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R7 /api/risk/abandoned-popular → 200 array", async () => {
    const res = await h.r7(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb("R4 /api/risk/stale-low-maintainer → 200 array", async () => {
    const res = await h.r4(req());
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  itDb(
    "R10 /api/risk/ranked → paginated TS-scored rows",
    async () => {
      const res = await h.r10(req());
      expect(res.status).toBe(200);

      const body = (await readJson(res)) as {
        items: Array<{
          package_id: string;
          package_name: string;
          risk_score: number;
          bucket: string;
        }>;
        total: number;
        limit: number;
        offset: number;
        hasMore: boolean;
      };

      expect(Array.isArray(body.items)).toBe(true);
      expect(body.items.length).toBeGreaterThan(0);
      expect(typeof body.total).toBe("number");
      expect(body.total).toBeGreaterThanOrEqual(body.items.length);
      expect(body.offset).toBe(0);

      for (let i = 1; i < body.items.length; i++) {
        expect(body.items[i - 1].risk_score).toBeGreaterThanOrEqual(
          body.items[i].risk_score,
        );
      }

      for (const row of body.items) {
        expect(row.risk_score).toBeGreaterThanOrEqual(0);
        expect(row.risk_score).toBeLessThanOrEqual(1);
        expect(["low", "medium", "high"]).toContain(row.bucket);
        expect(typeof row.package_name).toBe("string");
      }
    },
    { timeout: 90_000 },
  );
});

describe.skipIf(Boolean(process.env.SUPABASE_DB_URL))(
  "integration hint (SUPABASE_DB_URL not set)",
  () => {
    it("set SUPABASE_DB_URL in web/.env.local to run live API integration tests", () => {
      expect(process.env.SUPABASE_DB_URL).toBeFalsy();
    });
  },
);
