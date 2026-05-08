import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

let pathname = "/";

vi.mock("next/navigation", () => ({
  usePathname: () => pathname,
}));

import { SiteNav } from "@/components/site-nav";

describe("SiteNav", () => {
  beforeEach(() => {
    pathname = "/";
  });

  it("marks the exact matching item as current", () => {
    pathname = "/risk";

    render(<SiteNav />);

    expect(screen.getByRole("link", { name: "Risk" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(screen.getByRole("link", { name: "Graph" })).not.toHaveAttribute(
      "aria-current",
    );
  });

  it("keeps parent nav active for nested routes", () => {
    pathname = "/packages/pkg-1";

    render(<SiteNav />);

    expect(screen.getByRole("link", { name: "Packages" })).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("renders every top-level navigation target", () => {
    render(<SiteNav />);

    for (const label of [
      "Risk",
      "Graph",
      "Stats",
      "Packages",
      "Tracked",
      "Maintainers",
      "Abandoned",
      "No Repo",
    ]) {
      expect(screen.getByRole("link", { name: label })).toBeInTheDocument();
    }
  });
});
