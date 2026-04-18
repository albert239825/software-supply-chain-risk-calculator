import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: vi.fn(() => ({})),
}));

vi.mock("@/lib/db/rankings", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/db/rankings")>();
  return {
    ...actual,
    mostDependents: vi.fn(),
  };
});

import { mostDependents } from "@/lib/db/rankings";
import { GET } from "@/app/api/stats/most-dependents/route";

const mockedMostDependents = vi.mocked(mostDependents);

describe("GET /api/stats/most-dependents", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns rows ordered by dependents", async () => {
    mockedMostDependents.mockResolvedValueOnce([
      { package_name: "tslib", dependents: 1843 },
    ]);
    const req = new NextRequest(
      "http://localhost/api/stats/most-dependents?ecosystem=npm",
    );
    const res = await GET(req);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items).toEqual([
      { package_name: "tslib", dependents: 1843 },
    ]);
    // Default limit=10
    expect(body.meta).toEqual({ total: 1, limit: 10, offset: 0 });
    expect(mockedMostDependents).toHaveBeenCalledWith(expect.anything(), {
      ecosystem: "npm",
      limit: 10,
    });
  });

  it("rejects limit above the hard cap of 100 with 400", async () => {
    const req = new NextRequest(
      "http://localhost/api/stats/most-dependents?limit=250",
    );
    const res = await GET(req);
    expect(res.status).toBe(400);
    expect(mockedMostDependents).not.toHaveBeenCalled();
  });
});
