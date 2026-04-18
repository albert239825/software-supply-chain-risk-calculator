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

describe("GET /api/packages/:id/versions (R1)", () => {
  beforeEach(() => {
    createClientMock.mockReset();
  });

  it("returns a ListResponse<Version>", async () => {
    const rows = [
      {
        id: "v1",
        package_id: PKG_ID,
        version: "1.0.0",
        released: "2025-01-01T00:00:00Z",
        has_repository: true,
        github_owner: "o",
        github_repo: "r",
      },
    ];
    const { client } = createFakeSupabaseSequence([
      { data: PKG },
      { data: rows, count: 1 },
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/versions/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/versions`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items).toEqual(rows);
    expect(body.meta).toEqual({ total: 1, limit: 50, offset: 0 });
  });

  it("returns 404 when the package is unknown", async () => {
    const { client } = createFakeSupabaseSequence([{ data: null }]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/versions/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/versions`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(404);
  });

  it("returns 400 when the id is not a UUID", async () => {
    const { GET } = await import("@/app/api/packages/[id]/versions/route");
    const res = await GET(req(`/api/packages/xxx/versions`), {
      params: Promise.resolve({ id: "xxx" }),
    });
    expect(res.status).toBe(400);
  });

  it("returns 400 when limit is not a positive integer", async () => {
    const { GET } = await import("@/app/api/packages/[id]/versions/route");
    const res = await GET(
      req(`/api/packages/${PKG_ID}/versions?limit=-1`),
      { params: Promise.resolve({ id: PKG_ID }) },
    );
    expect(res.status).toBe(400);
  });
});
