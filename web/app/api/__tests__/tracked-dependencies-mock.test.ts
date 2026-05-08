import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

vi.mock("@/lib/auth", () => ({
  getCurrentUser: vi.fn(),
}));

import { GET, POST } from "@/app/api/tracked-dependencies/route";
import { getCurrentUser } from "@/lib/auth";
import pool from "@/lib/db";

describe("/api/tracked-dependencies", () => {
  beforeEach(() => {
    vi.mocked(getCurrentUser).mockReset();
    vi.mocked(pool.query).mockReset();
  });

  it("GET 401 when not logged in", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue(null);
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(401);
  });

  it("GET 200 returns rows for logged-in user", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: "e@x.com",
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [
        {
          id: "t1",
          package_id: "p1",
          note: null,
          created_at: "2020-01-01T00:00:00.000Z",
          updated_at: "2020-01-01T00:00:00.000Z",
          package_name: "left-pad",
          ecosystem: "npm",
          description: null,
          latest_version: "1.0.0",
          latest_version_id: "vid",
          last_release: "2020-06-01T00:00:00.000Z",
          has_repository: true,
          maintainer_count: "1",
          fanout_direct: "0",
          fanin_dependents: "0",
        },
      ],
    } as never);
    const res = await GET(new NextRequest("http://x"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as Array<{
      id: string;
      risk_score: number;
      risk_bucket: string;
      checked_at: string;
    }>;
    expect(body).toHaveLength(1);
    expect(body[0].id).toBe("t1");
    expect(body[0].risk_bucket).toMatch(/low|medium|high/);
    expect(typeof body[0].risk_score).toBe("number");
    expect(body[0].checked_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("POST 401 when not logged in", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue(null);
    const req = new NextRequest("http://x", {
      method: "POST",
      body: JSON.stringify({ packageId: "p1" }),
      headers: { "content-type": "application/json" },
    });
    const res = await POST(req);
    expect(res.status).toBe(401);
  });

  it("POST 400 when packageId missing", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    const req = new NextRequest("http://x", {
      method: "POST",
      body: JSON.stringify({}),
      headers: { "content-type": "application/json" },
    });
    const res = await POST(req);
    expect(res.status).toBe(400);
  });

  it("POST 201 persists a tracked dependency", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [
        {
          id: "row-1",
          package_id: "p1",
          note: "watch",
          created_at: "now",
          updated_at: "now",
        },
      ],
    } as never);

    const req = new NextRequest("http://x", {
      method: "POST",
      body: JSON.stringify({ packageId: "p1", note: " watch " }),
      headers: { "content-type": "application/json" },
    });
    const res = await POST(req);
    expect(res.status).toBe(201);
    const body = (await res.json()) as { package_id: string };
    expect(body.package_id).toBe("p1");
  });
});
