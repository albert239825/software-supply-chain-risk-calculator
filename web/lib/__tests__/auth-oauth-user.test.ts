import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import pool from "@/lib/db";
import {
  createOrUpdateUserFromOAuth,
  type OAuthProfile,
} from "@/lib/auth";

describe("createOrUpdateUserFromOAuth", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
  });

  const profile: OAuthProfile = {
    provider: "github",
    providerUserId: "gh-1",
    email: "dev@gmail.com",
    displayName: "Dev",
    avatarUrl: "http://a",
    profileUrl: "http://p",
  };

  it("links an existing identity to profile updates and returns the user row", async () => {
    vi.mocked(pool.query)
      .mockResolvedValueOnce({ rows: [{ user_id: "u-existing" }] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({
        rows: [
          {
            id: "u-existing",
            email: "dev@gmail.com",
            display_name: "Dev",
            avatar_url: "http://a",
          },
        ],
      } as never);

    const user = await createOrUpdateUserFromOAuth(profile);

    expect(user).toMatchObject({
      id: "u-existing",
      email: "dev@gmail.com",
      displayName: "Dev",
    });
    expect(pool.query).toHaveBeenCalledTimes(4);
  });

  it("creates a user when no identity or email match exists", async () => {
    vi.mocked(pool.query)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({ rows: [{ id: "u-new" }] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({
        rows: [
          {
            id: "u-new",
            email: "dev@gmail.com",
            display_name: "Dev",
            avatar_url: "http://a",
          },
        ],
      } as never);

    const user = await createOrUpdateUserFromOAuth(profile);

    expect(user.id).toBe("u-new");
    expect(pool.query).toHaveBeenCalledTimes(5);
  });

  it("reuses a user discovered by email when the identity is new", async () => {
    vi.mocked(pool.query)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({ rows: [{ id: "u-email" }] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({ rows: [] } as never)
      .mockResolvedValueOnce({
        rows: [
          {
            id: "u-email",
            email: "dev@gmail.com",
            display_name: "Dev",
            avatar_url: "http://a",
          },
        ],
      } as never);

    const user = await createOrUpdateUserFromOAuth(profile);

    expect(user.id).toBe("u-email");
    expect(pool.query).toHaveBeenCalledTimes(5);
  });
});
