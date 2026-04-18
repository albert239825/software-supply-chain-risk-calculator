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
  name: "express",
  description: "Fast, unopinionated, minimalist web framework",
  latest_version: "4.18.2",
};

function req(url: string): NextRequest {
  return new NextRequest(new URL(url, "http://localhost"));
}

describe("GET /api/packages/:id (A2)", () => {
  beforeEach(() => {
    createClientMock.mockReset();
  });

  it("returns the package enriched with latest-version fields", async () => {
    const { client } = createFakeSupabaseSequence([
      { data: PKG }, // getPackageById
      {
        data: {
          id: "v1",
          package_id: PKG_ID,
          version: "4.18.2",
          released: "2025-01-14T09:23:11Z",
          has_repository: true,
          github_owner: "expressjs",
          github_repo: "express",
        },
      }, // getLatestVersionRecord
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/route");
    const res = await GET(req(`/api/packages/${PKG_ID}`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toMatchObject({
      id: PKG_ID,
      name: "express",
      latest_version: "4.18.2",
      latest_released: "2025-01-14T09:23:11Z",
      latest_has_repository: true,
      latest_github_owner: "expressjs",
      latest_github_repo: "express",
    });
  });

  it("returns null latest_* fields when no matching version row exists", async () => {
    const { client } = createFakeSupabaseSequence([
      { data: PKG },
      { data: null },
    ]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/route");
    const res = await GET(req(`/api/packages/${PKG_ID}`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.latest_released).toBeNull();
    expect(body.latest_has_repository).toBeNull();
    expect(body.latest_github_owner).toBeNull();
    expect(body.latest_github_repo).toBeNull();
  });

  it("returns 404 when the package is unknown", async () => {
    const { client } = createFakeSupabaseSequence([{ data: null }]);
    createClientMock.mockReturnValue(client);

    const { GET } = await import("@/app/api/packages/[id]/route");
    const res = await GET(req(`/api/packages/${PKG_ID}`), {
      params: Promise.resolve({ id: PKG_ID }),
    });
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "Package not found" });
  });

  it("returns 400 when the id is not a valid UUID", async () => {
    const { GET } = await import("@/app/api/packages/[id]/route");
    const res = await GET(req(`/api/packages/not-a-uuid`), {
      params: Promise.resolve({ id: "not-a-uuid" }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toMatch(/UUID/);
  });
});
