import React from "react";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("next/link", () => ({
  default: ({
    children,
    href,
  }: {
    children: React.ReactNode;
    href: string;
  }) => <a href={href}>{children}</a>,
}));

import Home from "@/app/page";

describe("Home page", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        json: async () => ({
          user: {
            id: "u1",
            email: "viewer@example.com",
            displayName: "Viewer",
            avatarUrl: null,
          },
        }),
      } as Response),
    );
  });

  it("highlights the main workflow and navigation cards", async () => {
    render(<Home />);
    expect(
      await screen.findByRole("heading", {
        name: /evaluate dependency risk/i,
      }),
    ).toBeInTheDocument();
    expect(
      screen.getAllByRole("link", { name: /browse packages/i })[0],
    ).toHaveAttribute("href", "/packages");
    expect(screen.getAllByRole("link", { name: /Open$/ }).length).toBeGreaterThan(
      0,
    );
  });
});
