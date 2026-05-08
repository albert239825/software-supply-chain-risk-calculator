import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import pool from "@/lib/db";

describe("GET routes backed by pool (mocked)", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  it("/api/packages/all", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [{ package_name: "a", version: "1", released: null }],
    } as never);
    const { GET } = await import("@/app/api/packages/all/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
    const body = JSON.parse(await res.text()) as unknown[];
    expect(body).toHaveLength(1);
  });

  it("/api/packages/no-repo", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/no-repo/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/stats/top-fanout", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/stats/top-fanout/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/stats/most-dependents", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/stats/most-dependents/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/stats/depth-below", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/stats/depth-below/route");
    const res = await GET(new NextRequest("http://x?n=2"));
    expect(res.status).toBe(200);
  });

  it("/api/maintainers/top", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/maintainers/top/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/risk/abandoned-popular", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/risk/abandoned-popular/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/risk/stale-low-maintainer", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/risk/stale-low-maintainer/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
  });

  it("/api/risk/ranked — empty", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/risk/ranked/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
    const body = JSON.parse(await res.text()) as { items: unknown[]; total: number };
    expect(body.items).toEqual([]);
    expect(body.total).toBe(0);
  });

  it("/api/risk/ranked — scores rows", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [
        {
          package_id: "p1",
          package_name: "alpha",
          maintainer_count: "2",
          fanout_direct: "1",
          fanin_dependents: "1",
          last_release: new Date().toISOString(),
          has_repository: true,
        },
        {
          package_id: "p2",
          package_name: "beta",
          maintainer_count: "1",
          fanout_direct: "5",
          fanin_dependents: "10",
          last_release: null,
          has_repository: false,
        },
      ],
    } as never);
    const { GET } = await import("@/app/api/risk/ranked/route");
    const res = await GET(
      new NextRequest("http://x?limit=1&offset=0"),
    );
    expect(res.status).toBe(200);
    const body = JSON.parse(await res.text()) as {
      items: Array<{ package_id: string; risk_score: number }>;
      total: number;
      hasMore: boolean;
    };
    expect(body.total).toBe(2);
    expect(body.items).toHaveLength(1);
    expect(body.hasMore).toBe(true);
    expect(body.items[0].risk_score).toBeGreaterThanOrEqual(0);
  });

  const pkgCtx = { params: Promise.resolve({ packageId: "pid" }) };

  it("/api/packages/:id/versions", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/[packageId]/versions/route");
    const res = await GET(new NextRequest("http://x"), pkgCtx);
    expect(res.status).toBe(200);
  });

  it("/api/packages/:id/maintainers", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/[packageId]/maintainers/route");
    const res = await GET(new NextRequest("http://x"), pkgCtx);
    expect(res.status).toBe(200);
  });

  it("/api/packages/:id/dependencies", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/[packageId]/dependencies/route");
    const res = await GET(new NextRequest("http://x"), pkgCtx);
    expect(res.status).toBe(200);
  });

  it("/api/packages/:id/dependents", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/[packageId]/dependents/route");
    const res = await GET(new NextRequest("http://x"), pkgCtx);
    expect(res.status).toBe(200);
  });

  it("/api/packages/:id/graph respects maxDepth and maxOrder", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const { GET } = await import("@/app/api/packages/[packageId]/graph/route");
    const res = await GET(
      new NextRequest("http://x?maxDepth=2&maxOrder=3"),
      pkgCtx,
    );
    expect(res.status).toBe(200);
    expect(vi.mocked(pool.query).mock.calls[0][1]).toEqual([
      "pid",
      2,
      3,
    ]);
  });

  it("propagates SQL errors as 500", async () => {
    vi.mocked(pool.query).mockRejectedValueOnce(new Error("sql fail"));
    const { GET } = await import("@/app/api/packages/all/route");
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(500);
  });
});
