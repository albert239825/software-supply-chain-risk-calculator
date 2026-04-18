import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createFakeSupabaseSequence } from "@/lib/db/__tests__/_fake-supabase";
import type { Package } from "@/types/api";

const createClientMock = vi.fn();
vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: () => createClientMock(),
}));

const PKG_ID = "0063402a-1335-5d56-b371-0ac3026e129d";
const PKG: Package = {
  id: PKG_ID,
  ecosystem: "npm",
  name: "p",
  description: "",
  latest_version: "1.0.0",
};

function req(url: string): NextRequest {
  return new NextRequest(new URL(url, "http://localhost"));
}

describe("GET /api/packages/:id/maintainers (A3)", () => {
  beforeEach(() => {
    createClientMock.mockReset();
  });

  it("returns a ListResponse<Maintainer>", async () => {
    const rows = [
      { id: "m1", package_id: PKG_ID, username: "alice", name: "Alice", role: "owner", email: null },
      { id: "m2", package_id: PKG_ID, username: "bob", name: "Bob", role: "maintainer", email: null },
    ];
    const { client } = createFakeSupabaseSequence([
      { data: PKG },
      { data: rows },
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/maintainers/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/maintainers`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items.map((m: { username: string }) => m.username)).toEqual([
      "alice",
      "bob",
    ]);
    expect(body.meta).toEqual({ total: 2, limit: 100, offset: 0 });
  });

  it("returns 404 for unknown package", async () => {
    const { client } = createFakeSupabaseSequence([{ data: null }]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/maintainers/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/maintainers`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(404);
  });

  it("returns 400 for bad UUID", async () => {
    const { GET } = await import("@/app/api/packages/[id]/maintainers/route");
    const res = await GET(req(`/api/packages/xxx/maintainers`), {
      params: Promise.resolve({ id: "xxx" }),
    });
    expect(res.status).toBe(400);
  });
});
