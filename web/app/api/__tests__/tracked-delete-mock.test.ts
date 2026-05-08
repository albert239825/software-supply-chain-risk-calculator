import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

vi.mock("@/lib/auth", () => ({
  getCurrentUser: vi.fn(),
}));

import { DELETE } from "@/app/api/tracked-dependencies/[packageId]/route";
import { getCurrentUser } from "@/lib/auth";
import pool from "@/lib/db";

describe("DELETE /api/tracked-dependencies/:packageId", () => {
  beforeEach(() => {
    vi.mocked(getCurrentUser).mockReset();
    vi.mocked(pool.query).mockReset();
  });

  it("401 when anonymous", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue(null);
    const res = await DELETE(new NextRequest("http://x"), {
      params: Promise.resolve({ packageId: "p1" }),
    });
    expect(res.status).toBe(401);
  });

  it("404 when nothing deleted", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(pool.query).mockResolvedValueOnce({ rowCount: 0 } as never);
    const res = await DELETE(new NextRequest("http://x"), {
      params: Promise.resolve({ packageId: "p1" }),
    });
    expect(res.status).toBe(404);
  });

  it("200 when a row is removed", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(pool.query).mockResolvedValueOnce({ rowCount: 1 } as never);
    const res = await DELETE(new NextRequest("http://x"), {
      params: Promise.resolve({ packageId: "p1" }),
    });
    expect(res.status).toBe(200);
  });
});
