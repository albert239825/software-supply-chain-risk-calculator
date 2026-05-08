import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

import {
  fetchPackageJson,
  fetchRepos,
  getGitHubAccessToken,
} from "@/lib/github";
import pool from "@/lib/db";

describe("GitHub data access helpers", () => {
  beforeEach(() => {
    vi.mocked(pool.query).mockReset();
    vi.unstubAllGlobals();
  });

  it("getGitHubAccessToken returns the stored token", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({
      rows: [{ provider_access_token: "ghp_x" }],
    } as never);
    await expect(getGitHubAccessToken("user-1")).resolves.toBe("ghp_x");
  });

  it("getGitHubAccessToken returns null when missing", async () => {
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);
    await expect(getGitHubAccessToken("user-1")).resolves.toBeNull();
  });

  it("fetchRepos parses a successful GitHub response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => [
          {
            id: 1,
            name: "r",
            full_name: "org/r",
            private: false,
            default_branch: "main",
            html_url: "https://github.com/org/r",
            updated_at: "2024-01-01T00:00:00Z",
          },
        ],
        text: async () => "",
      } as Response),
    );

    const repos = await fetchRepos("token");
    expect(repos).toHaveLength(1);
    expect(repos[0].full_name).toBe("org/r");
  });

  it("fetchPackageJson decodes base64 package.json contents", async () => {
    const encoded = Buffer.from(
      JSON.stringify({ name: "demo", dependencies: { left: "1" } }),
      "utf8",
    ).toString("base64");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ encoding: "base64", content: encoded }),
        text: async () => "",
      } as Response),
    );

    const pkg = await fetchPackageJson("token", "org/r");
    expect(pkg.name).toBe("demo");
    expect(pkg.dependencies).toEqual({ left: "1" });
  });

  it("fetchPackageJson rejects non-base64 payloads", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ encoding: "utf-8", content: "{}" }),
        text: async () => "",
      } as Response),
    );

    await expect(fetchPackageJson("token", "org/r")).rejects.toThrow(
      /base64/,
    );
  });
});
