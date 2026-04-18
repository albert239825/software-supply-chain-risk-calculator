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
const LATEST = {
  id: "v1",
  package_id: PKG_ID,
  version: "1.0.0",
  released: "2025-01-01T00:00:00Z",
  has_repository: true,
  github_owner: "o",
  github_repo: "r",
};

function req(url: string): NextRequest {
  return new NextRequest(new URL(url, "http://localhost"));
}

describe("GET /api/packages/:id/dependencies (A4)", () => {
  beforeEach(() => {
    createClientMock.mockReset();
  });

  it("returns a ListResponse<DirectDependencyRow>", async () => {
    const rows = [
      { to_package_id: "to-1", to_package: "cookie", version_spec: "0.4.2", dep_kind: "dependency" },
    ];
    const { client } = createFakeSupabaseSequence([
      { data: PKG }, // getPackageById
      { data: LATEST }, // getLatestVersionRecord (latestVersion provided -> versions)
      { data: rows, count: 1 }, // listDirectDependencies
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/dependencies/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/dependencies`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.items).toEqual(rows);
    expect(body.meta).toEqual({ total: 1, limit: 100, offset: 0 });
  });

  it("returns 404 when the package has no latest version row", async () => {
    const { client } = createFakeSupabaseSequence([
      { data: PKG },
      { data: null },
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/dependencies/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/dependencies`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(404);
  });

  it("returns 404 when the package is unknown", async () => {
    const { client } = createFakeSupabaseSequence([{ data: null }]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/dependencies/route");
    const res = await GET(req(`/api/packages/${PKG_ID}/dependencies`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(404);
  });

  it("returns 400 for invalid depKind", async () => {
    const { GET } = await import("@/app/api/packages/[id]/dependencies/route");
    const res = await GET(
      req(`/api/packages/${PKG_ID}/dependencies?depKind=bogus`),
      { params: Promise.resolve({ id: PKG_ID }) },
    );
    expect(res.status).toBe(400);
  });
});
