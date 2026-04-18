import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createFakeSupabase } from "@/lib/db/__tests__/_fake-supabase";

const createClientMock = vi.fn();
vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: () => createClientMock(),
}));

function req(url: string): NextRequest {
  return new NextRequest(new URL(url, "http://localhost"));
}

describe("GET /api/packages/no-repo (R9)", () => {
  beforeEach(() => {
    createClientMock.mockReset();
  });

  it("returns a ListResponse of { package_name, version }", async () => {
    const rows = [{ package_name: "a", version: "1.0.0" }];
    const { client } = createFakeSupabase(() => ({ data: rows, count: 1 }));
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/no-repo/route");
    const res = await GET(req("/api/packages/no-repo"));
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items).toEqual(rows);
    expect(body.meta).toEqual({ total: 1, limit: 100, offset: 0 });
  });

  it("returns 400 when limit is negative", async () => {
    const { GET } = await import("@/app/api/packages/no-repo/route");
    const res = await GET(req("/api/packages/no-repo?limit=-5"));
    expect(res.status).toBe(400);
  });

  it("returns 500 when the helper throws", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "db down" },
    }));
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/no-repo/route");
    const res = await GET(req("/api/packages/no-repo"));
    expect(res.status).toBe(500);
  });
});
