import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: vi.fn(() => ({})),
}));

vi.mock("@/lib/db/rankings", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/db/rankings")>();
  return {
    ...actual,
    topMaintainers: vi.fn(),
  };
});

import { topMaintainers } from "@/lib/db/rankings";
import { GET } from "@/app/api/maintainers/top/route";

const mockedTopMaintainers = vi.mocked(topMaintainers);

describe("GET /api/maintainers/top", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns top maintainers with default limit", async () => {
    mockedTopMaintainers.mockResolvedValueOnce([
      { username: "sindresorhus", num_packages: 917 },
      { username: "alice", num_packages: 12 },
    ]);
    const req = new NextRequest(
      "http://localhost/api/maintainers/top?ecosystem=npm",
    );
    const res = await GET(req);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items).toHaveLength(2);
    expect(body.meta).toEqual({ total: 2, limit: 10, offset: 0 });
  });

  it("rejects unknown ecosystem with 400", async () => {
    const req = new NextRequest(
      "http://localhost/api/maintainers/top?ecosystem=maven",
    );
    const res = await GET(req);
    expect(res.status).toBe(400);
    expect(mockedTopMaintainers).not.toHaveBeenCalled();
  });
});
