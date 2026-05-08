import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("next/dynamic", () => ({
  default: () =>
    function GraphDynStub() {
      return <div data-testid="graph-dyn">visualization</div>;
    },
}));

import GraphExplorer from "@/app/graph/page";

const fetchMock = vi.fn();

describe("GraphExplorer page", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockImplementation((input: RequestInfo | URL) => {
      const u = String(input);
      if (u.includes("/api/graph/seeds")) {
        return Promise.resolve({
          ok: true,
          json: async () => [],
        } as Response);
      }
      if (u.includes("/api/graph/search")) {
        return Promise.resolve({
          ok: true,
          json: async () => [],
        } as Response);
      }
      return Promise.resolve({
        ok: true,
        json: async () => [],
      } as Response);
    });
    vi.stubGlobal("fetch", fetchMock);
  });

  it("loads seeds and shows the explorer chrome", async () => {
    globalThis.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    };

    render(<GraphExplorer />);
    expect(await screen.findByText("Graph Explorer")).toBeInTheDocument();
    await waitFor(() =>
      expect(fetchMock.mock.calls.some((c) => String(c[0]).includes("/api/graph/seeds"))).toBe(
        true,
      ),
    );
  });
});
