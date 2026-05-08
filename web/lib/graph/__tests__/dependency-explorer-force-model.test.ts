import { describe, expect, it } from "vitest";

import {
  coerceDependencyEdgeRows,
  dependencyRowsToPackageForceGraph,
  inferRootPackageFromEdges,
  type DependencyGraphEdgeRow,
} from "@/lib/graph/dependency-explorer-force-model";

const baseRow = {
  from_package_id: "p1",
  from_version_id: "v1",
  to_package_id: "p2",
  from_package: "a",
  from_version: "1.0.0",
  to_package: "b",
  version_spec: "^1",
  dep_kind: "prod",
  depth: 1,
};

describe("coerceDependencyEdgeRows", () => {
  it("returns empty for non-array", () => {
    expect(coerceDependencyEdgeRows(null)).toEqual([]);
    expect(coerceDependencyEdgeRows({})).toEqual([]);
  });

  it("drops invalid depth and ids", () => {
    expect(
      coerceDependencyEdgeRows([{ ...baseRow, depth: 0 }]),
    ).toEqual([]);
    expect(
      coerceDependencyEdgeRows([{ ...baseRow, from_package_id: "" }]),
    ).toEqual([]);
  });

  it("accepts depth as string and caps at 98", () => {
    const rows = coerceDependencyEdgeRows([
      { ...baseRow, depth: "99" },
    ]);
    expect(rows[0]?.depth).toBe(98);
  });

  it("preserves valid rows", () => {
    const rows = coerceDependencyEdgeRows([baseRow]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      from_package_id: "p1",
      to_package_id: "p2",
      depth: 1,
    });
  });
});

describe("inferRootPackageFromEdges", () => {
  it("returns null when depth-1 parents disagree", () => {
    const rows: DependencyGraphEdgeRow[] = [
      { ...baseRow, depth: 1, from_package_id: "a" },
      { ...baseRow, depth: 1, from_package_id: "b", to_package_id: "c" },
    ];
    expect(inferRootPackageFromEdges(rows)).toBeNull();
  });

  it("returns the sole depth-1 parent id", () => {
    const rows: DependencyGraphEdgeRow[] = [
      { ...baseRow, depth: 1, from_package_id: "root" },
      { ...baseRow, depth: 2, from_package_id: "root", to_package_id: "c" },
    ];
    expect(inferRootPackageFromEdges(rows)).toBe("root");
  });
});

describe("dependencyRowsToPackageForceGraph", () => {
  it("builds nodes and merged links", () => {
    const rows: DependencyGraphEdgeRow[] = [
      {
        ...baseRow,
        depth: 1,
        from_package_id: "r",
        to_package_id: "c",
        from_package: "root",
        to_package: "child",
      },
    ];
    const { nodes, links } = dependencyRowsToPackageForceGraph(rows, "r");
    expect(nodes.some((n) => n.isRoot)).toBe(true);
    expect(links.some((l) => l.source === "p:r" && l.target === "p:c")).toBe(
      true,
    );
    expect(links[0]?.depKinds.length).toBeGreaterThan(0);
  });

  it("uses fallback tiers when root unknown", () => {
    const rows: DependencyGraphEdgeRow[] = [
      { ...baseRow, depth: 1, from_package_id: "a", to_package_id: "b" },
    ];
    const { nodes } = dependencyRowsToPackageForceGraph(rows, null);
    expect(nodes.length).toBeGreaterThanOrEqual(2);
  });
});
