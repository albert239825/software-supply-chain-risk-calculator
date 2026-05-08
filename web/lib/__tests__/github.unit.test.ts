import { describe, expect, it, vi } from "vitest";

import {
  extractPackageJsonDependencies,
  fetchGitHubJson,
} from "@/lib/github";

describe("extractPackageJsonDependencies", () => {
  it("collects dependency keys from common sections", () => {
    const names = extractPackageJsonDependencies({
      dependencies: { Lodash: "^4", express: "4" },
      devDependencies: { Jest: "29" },
      peerDependencies: { react: "18" },
      optionalDependencies: { fsevents: "2" },
      unrelated: "skip",
    });
    expect(names).toEqual(["express", "fsevents", "jest", "lodash", "react"]);
  });

  it("ignores non-object sections", () => {
    expect(
      extractPackageJsonDependencies({
        dependencies: ["nope"],
        devDependencies: null,
      } as Record<string, unknown>),
    ).toEqual([]);
  });
});

describe("fetchGitHubJson", () => {
  it("throws a concise error on HTTP failures", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        text: async () => "nope",
      } as Response),
    );
    await expect(fetchGitHubJson("tok", "nope:example")).rejects.toThrow(
      /GitHub API error 401/,
    );
  });
});
