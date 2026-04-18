import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: vi.fn(() => ({})),
}));

vi.mock("@/lib/db/rankings", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/db/rankings")>();
  return {
    ...actual,
    topFanout: vi.fn(),
  };
});

import { topFanout } from "@/lib/db/rankings";
import { GET } from "@/app/api/stats/top-fanout/route";

const mockedTopFanout = vi.mocked(topFanout);

describe("GET /api/stats/top-fanout", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns the top-N fan-out rows with meta", async () => {
    mockedTopFanout.mockResolvedValueOnce([
      { package_name: "webpack", num_dependencies: 34 },
      { package_name: "next", num_dependencies: 21 },
    ]);
    const req = new NextRequest(
      "http://localhost/api/stats/top-fanout?limit=5&ecosystem=npm",
    );
    const res = await GET(req);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({
      items: [
        { package_name: "webpack", num_dependencies: 34 },
        { package_name: "next", num_dependencies: 21 },
      ],
      meta: { total: 2, limit: 5, offset: 0 },
    });
    expect(mockedTopFanout).toHaveBeenCalledWith(expect.anything(), {
      ecosystem: "npm",
      limit: 5,
    });
  });

  it("rejects non-numeric limit with 400", async () => {
    const req = new NextRequest(
      "http://localhost/api/stats/top-fanout?limit=abc",
    );
    const res = await GET(req);
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toMatch(/limit/);
    expect(mockedTopFanout).not.toHaveBeenCalled();
  });
});
