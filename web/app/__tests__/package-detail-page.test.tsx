import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useParams: () => ({ packageId: "pkg-1" }),
}));

import PackageDetailPage from "@/app/packages/[packageId]/page";

const fetchMock = vi.fn();

describe("PackageDetailPage", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  it("renders JSON payload for versions", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => [{ version: "1.0.0" }],
    } as Response);

    render(<PackageDetailPage />);
    await waitFor(() => {
      const pre = document.querySelector("pre");
      expect(pre?.textContent).toContain("1.0.0");
    });
  });
});
