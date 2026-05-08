import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import { GET as graphSearch } from "@/app/api/graph/search/route";
import { GET as graphSeeds } from "@/app/api/graph/seeds/route";
import pool from "@/lib/db";

describe("/api/graph/search", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  it("returns [] for queries shorter than 2 chars", async () => {
    const res = await graphSearch(
      new NextRequest("http://localhost/api/graph/search?q=a"),
    );
    expect(res.status).toBe(200);
    expect(JSON.parse(await res.text())).toEqual([]);
    expect(pool.query).not.toHaveBeenCalled();
  });

  it("filters out rows without latest_version_id", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [
        {
          package_id: "p1",
          package_name: "leftpad",
          ecosystem: "npm",
          latest_version: "1.0.0",
          latest_version_id: null,
        },
        {
          package_id: "p2",
          package_name: "express",
          ecosystem: "npm",
          latest_version: "4",
          latest_version_id: "vid",
        },
      ],
    } as never);

    const res = await graphSearch(
      new NextRequest("http://localhost/api/graph/search?q=ex"),
    );
    expect(res.status).toBe(200);
    const body = JSON.parse(await res.text()) as unknown[];
    expect(body).toHaveLength(1);
    expect((body[0] as { package_id: string }).package_id).toBe("p2");
  });

  it("returns 500 when the database errors", async () => {
    vi.mocked(pool.query).mockRejectedValueOnce(new Error("boom"));
    const res = await graphSearch(
      new NextRequest("http://localhost/api/graph/search?q=ab"),
    );
    expect(res.status).toBe(500);
  });
});

describe("/api/graph/seeds", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  it("clamps limit into [1, 48]", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    await graphSeeds(
      new NextRequest("http://localhost/api/graph/seeds?limit=999"),
    );
    expect(vi.mocked(pool.query).mock.calls[0][1]).toEqual([48]);
  });

  it("maps dependency_count to a number", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [
        {
          package_id: "p",
          version_id: "v",
          ecosystem: "npm",
          package_name: "x",
          version: "1",
          dependency_count: "42",
        },
      ],
    } as never);

    const res = await graphSeeds(new NextRequest("http://localhost/api/graph/seeds"));
    const body = JSON.parse(await res.text()) as Array<{ dependency_count: number }>;
    expect(body[0].dependency_count).toBe(42);
  });
});
