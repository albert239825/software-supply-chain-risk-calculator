import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

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
  it("highlights the main workflow and navigation cards", () => {
    render(<Home />);
    expect(
      screen.getByRole("heading", {
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
