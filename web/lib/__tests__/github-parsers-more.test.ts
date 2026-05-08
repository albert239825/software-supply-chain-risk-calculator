import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  createDependencySets,
  extractPackageLockDependencies,
  extractPyprojectDependencies,
  extractRequirementsDependencies,
  fetchRepoFileText,
  findDependencyManifestPaths,
} from "@/lib/github";

describe("createDependencySets", () => {
  it("returns fresh empty npm and pypi sets", () => {
    const s = createDependencySets();
    expect(s.npm.size).toBe(0);
    expect(s.pypi.size).toBe(0);
  });
});

describe("extractPackageLockDependencies", () => {
  it("collects npm package names from dependencies and packages maps", () => {
    const names = extractPackageLockDependencies({
      dependencies: { Express: "4" },
      packages: {
        "node_modules/lodash": {},
        "packages/foo/node_modules/bar": {},
      },
    });
    expect(names).toContain("express");
    expect(names).toContain("lodash");
    expect(names).toContain("bar");
  });

  it("returns an empty list when lockfile has no dep sections", () => {
    expect(extractPackageLockDependencies({})).toEqual([]);
  });
});

describe("extractRequirementsDependencies", () => {
  it("parses requirement lines and normalizes names", () => {
    expect(
      extractRequirementsDependencies("Django>=3.2\n# comment\n\nFlask==2"),
    ).toEqual(["django", "flask"]);
  });

  it("skips dash-prefixed tooling lines", () => {
    expect(extractRequirementsDependencies("-r base.txt")).toEqual([]);
  });
});

describe("extractPyprojectDependencies", () => {
  it("reads quoted dependency names from dependency arrays", () => {
    const text = `[project]\ndependencies = [ "httpx", "pydantic>=2", 'toolz' ]`;
    const deps = extractPyprojectDependencies(text);
    expect(deps).toEqual(["httpx", "pydantic", "toolz"]);
  });
});

describe("findDependencyManifestPaths", () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns sorted manifests and prefers root package.json", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          tree: [
            { path: "apps/web/package.json", type: "blob" },
            { path: "package.json", type: "blob" },
            { path: "package-lock.json", type: "blob" },
            { path: "src/requirements.txt", type: "blob" },
            { path: "pyproject.toml", type: "blob" },
            { path: "node_modules/left-pad/package.json", type: "blob" },
            { path: "readme.md", type: "blob" },
            { path: "folder", type: "tree" },
          ],
        }),
        text: async () => "",
      } as Response),
    );

    const manifests = await findDependencyManifestPaths("tok", "org/repo");
    expect(manifests[0]).toEqual({
      path: "package.json",
      kind: "package.json",
    });
    expect(manifests.some((m) => m.kind === "requirements.txt")).toBe(true);
    expect(manifests.some((m) => m.kind === "pyproject.toml")).toBe(true);
    expect(manifests.some((m) => m.kind === "package-lock.json")).toBe(true);
    expect(
      manifests.some((m) => m.path.includes("node_modules")),
    ).toBe(false);
  });
});

describe("fetchRepoFileText", () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it("requests URL-encoded path segments and decodes base64 body", async () => {
    const plain = "export {}";
    const b64 = Buffer.from(plain, "utf8").toString("base64");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ encoding: "base64", content: b64 }),
        text: async () => "",
      } as Response),
    );

    const text = await fetchRepoFileText("tok", "o/r", "packages/a file.ts");
    expect(text).toBe(plain);
    expect(vi.mocked(fetch).mock.calls[0][0]).toContain(
      encodeURIComponent("a file.ts"),
    );
  });

  it("throws when GitHub returns non-base64 content", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ encoding: "utf-8", content: "nope" }),
        text: async () => "",
      } as Response),
    );

    await expect(
      fetchRepoFileText("tok", "o/r", "x.txt"),
    ).rejects.toThrow(/base64/);
  });
});
