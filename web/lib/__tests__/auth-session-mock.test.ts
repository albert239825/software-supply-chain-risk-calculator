import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import pool from "@/lib/db";
import {
  SESSION_COOKIE,
  createSession,
  deleteSession,
  getCurrentUser,
  hashToken,
} from "@/lib/auth";

describe("auth session (mocked pool)", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  it("getCurrentUser returns null without session cookie", async () => {
    expect(await getCurrentUser(new Request("http://x"))).toBeNull();
    expect(pool.query).not.toHaveBeenCalled();
  });

  it("getCurrentUser returns null when session is unknown", async () => {
    const token = "nope";
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const req = new Request("http://x", {
      headers: { cookie: `${SESSION_COOKIE}=${encodeURIComponent(token)}` },
    });
    expect(await getCurrentUser(req)).toBeNull();
  });

  it("getCurrentUser returns user and touches last_seen when session valid", async () => {
    const token = "abc";
    vi.mocked(pool.query)
      .mockResolvedValueOnce({
        rows: [
          {
            user_id: "u1",
            email: "e@x.com",
            display_name: "E",
            avatar_url: null,
          },
        ],
      } as never)
      .mockResolvedValueOnce({ rows: [] } as never);

    const req = new Request("http://x", {
      headers: { cookie: `${SESSION_COOKIE}=${token}` },
    });
    const user = await getCurrentUser(req);
    expect(user).toMatchObject({
      id: "u1",
      email: "e@x.com",
      displayName: "E",
      avatarUrl: null,
    });
    expect(pool.query).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pool.query).mock.calls[0][1]).toEqual([hashToken(token)]);
  });

  it("deleteSession is noop without cookie", async () => {
    await deleteSession(new Request("http://x"));
    expect(pool.query).not.toHaveBeenCalled();
  });

  it("deleteSession removes row when cookie present", async () => {
    const token = "t";
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    await deleteSession(
      new Request("http://x", {
        headers: { cookie: `${SESSION_COOKIE}=${token}` },
      }),
    );
    expect(vi.mocked(pool.query).mock.calls[0][1]).toEqual([hashToken(token)]);
  });

  it("createSession inserts a session and returns the opaque token", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    const token = await createSession("user-99");
    expect(token.length).toBeGreaterThan(20);
    expect(vi.mocked(pool.query).mock.calls[0][1]).toEqual([
      "user-99",
      hashToken(token),
      30,
    ]);
  });
});
