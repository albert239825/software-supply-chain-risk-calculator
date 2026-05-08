import React from "react";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("react-force-graph-2d", () => ({
  default: React.forwardRef(function MockForceGraph(props: {
    graphData?: { nodes: unknown[] };
    onNodeClick?: (node: unknown) => void;
  }) {
    React.useEffect(() => {
      const nodes = props.graphData?.nodes;
      if (nodes?.length && props.onNodeClick) {
        props.onNodeClick(nodes[0]);
      }
    }, [props]);
    return <div data-testid="force-graph" />;
  }),
}));

import { DependencyExplorerGraph } from "@/components/dependency-explorer-graph";

class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

const sampleNode = {
  id: "p:r",
  uuid: "root-uuid",
  name: "root-pkg",
  subtitle: "package · root-uuid",
  tier: 0,
  isRoot: true,
  val: 6,
  color: "#2563eb",
};

describe("DependencyExplorerGraph", () => {
  beforeEach(() => {
    globalThis.ResizeObserver = ResizeObserverStub;
  });

  it("shows guidance when there are no edges", () => {
    render(<DependencyExplorerGraph graphData={{ nodes: [], links: [] }} />);
    expect(screen.getByText(/No dependency edges returned/i)).toBeInTheDocument();
  });

  it("mounts the force layout when nodes exist", () => {
    render(
      <DependencyExplorerGraph
        graphData={{
          nodes: [sampleNode],
          links: [],
        }}
      />,
    );
    expect(screen.getByTestId("force-graph")).toBeInTheDocument();
    expect(screen.getByText(/root package/i)).toBeInTheDocument();
    expect(screen.getByText("root-pkg")).toBeInTheDocument();
  });
});
