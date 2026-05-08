import React from "react";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const zoomToFitMock = vi.fn();
const d3ReheatSimulationMock = vi.fn();

vi.mock("react-force-graph-2d", () => ({
  default: React.forwardRef(function MockForceGraph(props: {
    graphData?: { nodes: unknown[]; links: unknown[] };
    onNodeClick?: (node: unknown) => void;
    onBackgroundClick?: () => void;
    linkColor?: (link: unknown) => string;
    linkDirectionalArrowColor?: (link: unknown) => string;
    linkLabel?: (link: unknown) => string;
    nodeLabel?: (node: unknown) => string;
    nodeVal?: (node: unknown) => number;
    nodeColor?: (node: unknown) => string;
  }, ref: React.ForwardedRef<unknown>) {
    React.useImperativeHandle(ref, () => ({
      zoomToFit: zoomToFitMock,
      d3ReheatSimulation: d3ReheatSimulationMock,
    }));

    const firstNode = props.graphData?.nodes[0];
    const firstLink = props.graphData?.links[0];
    const longLink = { label: "x".repeat(300), depKinds: "not-an-array" };

    const linkColor = firstLink ? props.linkColor?.(firstLink) : "";
    const arrowColor = props.linkDirectionalArrowColor?.(longLink);
    const linkLabel = props.linkLabel?.(longLink);
    const nodeLabel = firstNode ? props.nodeLabel?.(firstNode) : "";
    const nodeVal = firstNode ? props.nodeVal?.(firstNode) : 0;
    const nodeColor = firstNode ? props.nodeColor?.(firstNode) : "";

    return (
      <div data-testid="force-graph">
        <output data-testid="link-color">{linkColor}</output>
        <output data-testid="arrow-color">{arrowColor}</output>
        <output data-testid="link-label">{linkLabel}</output>
        <output data-testid="node-label">{nodeLabel}</output>
        <output data-testid="node-val">{nodeVal}</output>
        <output data-testid="node-color">{nodeColor}</output>
        <button type="button" onClick={() => firstNode && props.onNodeClick?.(firstNode)}>
          focus node
        </button>
        <button type="button" onClick={props.onBackgroundClick}>
          clear focus
        </button>
      </div>
    );
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
    zoomToFitMock.mockClear();
    d3ReheatSimulationMock.mockClear();
    globalThis.ResizeObserver = ResizeObserverStub;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
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
          links: [
            {
              source: "p:r",
              target: "p:d",
              label: "dependency (^1)",
              depKinds: ["dependency"],
            },
          ],
        }}
      />,
    );
    expect(screen.getByTestId("force-graph")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /focus node/i }));
    expect(screen.getByText(/root package/i)).toBeInTheDocument();
    expect(screen.getByText("root-pkg")).toBeInTheDocument();
    expect(screen.getByTestId("link-color").textContent).toMatch(/^#/);
    expect(screen.getByTestId("arrow-color").textContent).toMatch(/^#/);
    expect(screen.getByTestId("link-label").textContent).toHaveLength(281);
    expect(screen.getByTestId("node-label").innerHTML).toContain("root-pkg");
    expect(screen.getByTestId("node-val").textContent).toBe("6");
    expect(screen.getByTestId("node-color").textContent).toBe("#2563eb");

    fireEvent.click(screen.getByRole("button", { name: /clear focus/i }));
    expect(screen.queryByText(/root package/i)).not.toBeInTheDocument();
  });

  it("zooms to fit after graph data arrives", () => {
    vi.useFakeTimers();

    render(
      <DependencyExplorerGraph
        graphData={{
          nodes: [sampleNode],
          links: [],
        }}
      />,
    );

    act(() => {
      vi.advanceTimersByTime(60);
    });

    expect(zoomToFitMock).toHaveBeenCalledWith(520, 40);
    expect(d3ReheatSimulationMock).toHaveBeenCalled();
  });

  it("falls back when ResizeObserver is not available", () => {
    vi.stubGlobal("ResizeObserver", undefined);

    render(
      <DependencyExplorerGraph
        graphData={{
          nodes: [sampleNode],
          links: [],
        }}
      />,
    );

    expect(screen.getByTestId("force-graph")).toBeInTheDocument();
  });
});
