import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import { GET } from "@/app/api/packages/[packageId]/route";
import pool from "@/lib/db";

describe("GET /api/packages/:packageId (mocked)", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  it("404 when package not found", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const res = await GET(new NextRequest("http://x"), {
      params: Promise.resolve({ packageId: "missing" }),
    });
    expect(res.status).toBe(404);
    const body = JSON.parse(await res.text()) as { error: string };
    expect(body.error).toBe("package not found");
  });

  it("200 returns the first row", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [{ id: "p1", name: "lodash", ecosystem: "npm" }],
    } as never);
    const res = await GET(new NextRequest("http://x"), {
      params: Promise.resolve({ packageId: "p1" }),
    });
    expect(res.status).toBe(200);
    const body = JSON.parse(await res.text()) as { id: string; name: string };
    expect(body.id).toBe("p1");
    expect(body.name).toBe("lodash");
  });
});
