import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/auth", async (importOriginal) => {
  const mod = await importOriginal<typeof import("@/lib/auth")>();
  return {
    ...mod,
    deleteSession: vi.fn().mockResolvedValue(undefined),
  };
});

import { POST } from "@/app/api/auth/logout/route";
import { deleteSession } from "@/lib/auth";

describe("POST /api/auth/logout", () => {
  beforeEach(() => {
    vi.mocked(deleteSession).mockClear();
  });

  it("clears session server-side and sets cookie", async () => {
    const res = await POST(new NextRequest("http://localhost/api/auth/logout"));
    expect(res.status).toBe(200);
    expect(deleteSession).toHaveBeenCalled();
    const body = (await res.json()) as { ok: boolean };
    expect(body.ok).toBe(true);
    expect(res.cookies.get("ssc_session")?.value).toBe("");
  });

  it("returns 500 when deleteSession throws", async () => {
    vi.mocked(deleteSession).mockRejectedValueOnce(new Error("nope"));
    const res = await POST(new NextRequest("http://localhost/api/auth/logout"));
    expect(res.status).toBe(500);
  });
});
