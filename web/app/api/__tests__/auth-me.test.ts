import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/auth", () => ({
  getCurrentUser: vi.fn(),
}));

import { GET } from "@/app/api/auth/me/route";
import { getCurrentUser } from "@/lib/auth";

describe("GET /api/auth/me", () => {
  beforeEach(() => {
    vi.mocked(getCurrentUser).mockReset();
  });

  it("returns the current user as JSON", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: "a@b.com",
      displayName: "A",
      avatarUrl: null,
    });
    const res = await GET(new NextRequest("http://localhost/api/auth/me"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { user: { id: string } };
    expect(body.user.id).toBe("u1");
  });

  it("returns 500 when getCurrentUser throws", async () => {
    vi.mocked(getCurrentUser).mockRejectedValue(new Error("db down"));
    const res = await GET(new NextRequest("http://localhost/api/auth/me"));
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("db down");
  });
});
