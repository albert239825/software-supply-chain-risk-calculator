"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Activity, Bell, GitBranch, Mail, PackageSearch } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";

type User = {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
};

const modules = [
  {
    name: "Browse Packages",
    blurb: "Browse collected package versions and open package detail pages.",
    href: "/packages",
    icon: PackageSearch,
  },
  {
    name: "Risk Rankings",
    blurb: "Compare composite risk scores across maintainers, staleness, dependencies, and repo data.",
    href: "/risk",
    icon: Activity,
  },
  {
    name: "Dependency Graph",
    blurb: "Search packages and inspect first- and second-order dependency edges.",
    href: "/graph",
    icon: GitBranch,
  },
  {
    name: "Tracked Dependencies",
    blurb: "Save packages and import dependency lists from GitHub repositories.",
    href: "/track",
    icon: Bell,
  },
];

const signals = [
  { label: "Risk score", value: "Composite package health ranking" },
  { label: "Graph traversal", value: "Interactive dependency paths" },
  { label: "Postgres-backed", value: "Normalized Supabase query routes" },
  { label: "Tracking", value: "User saved dependency lists" },
];

export default function Home() {
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    fetch("/api/auth/me")
      .then((res) => res.json())
      .then((body: { user: User | null }) => {
        setUser(body.user);
      })
      .catch(() => {});
  }, []);

  function login(provider: "google" | "github") {
    window.location.href = `/api/auth/${provider}`;
  }

  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-10 px-6 py-12 lg:gap-12 lg:py-16">
      <div className="grid items-start gap-8 lg:grid-cols-2 lg:gap-10">
        <header className="flex flex-col gap-4">
          <p className="text-muted-foreground text-xs font-semibold uppercase tracking-wider">
            Software supply chain analysis
          </p>
          <h1 className="text-balance text-3xl font-semibold tracking-tight sm:text-4xl">
            Evaluate dependency risk across package ecosystems.
          </h1>
          <p className="text-muted-foreground text-pretty text-base leading-relaxed sm:text-lg">
            Keep an eye on the dependencies that matter, compare package risk,
            inspect dependency paths, and return to saved watch lists.
          </p>
        </header>

        <div className="lg:pt-1">
          {!user && (
            <Card className="border-border shadow-sm">
              <CardHeader className="space-y-1">
                <CardTitle className="text-xl">Sign in to track dependencies</CardTitle>
                <CardDescription className="text-base leading-relaxed">
                  Google and GitHub login unlock saved package tracking and repository imports.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-3 sm:flex-row sm:flex-wrap">
                <Button className="sm:flex-1" onClick={() => login("google")}>
                  <Mail />
                  Continue with Gmail
                </Button>
                <Button
                  className="sm:flex-1"
                  variant="outline"
                  onClick={() => login("github")}
                >
                  Continue with GitHub
                </Button>
              </CardContent>
            </Card>
          )}

          {user && (
            <Card className="border-border shadow-sm">
              <CardHeader className="space-y-1">
                <CardTitle className="text-xl">
                  Signed in as {user.displayName || user.email || "your account"}
                </CardTitle>
                <CardDescription className="text-base leading-relaxed">
                  Saved dependencies and GitHub imports are available from the tracking page.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-3 sm:flex-row sm:flex-wrap">
                <Button asChild className="sm:flex-1">
                  <Link href="/track">
                    <Bell />
                    Track dependencies
                  </Link>
                </Button>
                <Button asChild className="sm:flex-1" variant="outline">
                  <Link href="/packages">Browse packages</Link>
                </Button>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {signals.map((signal) => (
          <Card key={signal.label} className="border-border shadow-sm">
            <CardHeader className="pb-3 pt-4">
              <CardTitle className="text-sm font-semibold">{signal.label}</CardTitle>
              <CardDescription className="text-sm leading-snug">
                {signal.value}
              </CardDescription>
            </CardHeader>
          </Card>
        ))}
      </div>

      <Card className="border-border shadow-sm">
        <CardHeader>
          <CardTitle className="text-xl">Start exploring</CardTitle>
          <CardDescription className="max-w-2xl text-base leading-relaxed">
            Main database-backed views for package risk, graph structure, and saved tracking.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ul className="grid gap-3 sm:grid-cols-2">
            {modules.map((module) => {
              const Icon = module.icon;
              return (
                <li key={module.name}>
                  <Link
                    href={module.href}
                    className="group flex h-full gap-3 rounded-lg border border-border bg-card/50 p-4 transition-colors hover:border-primary/30 hover:bg-accent/50"
                  >
                    <span className="flex size-10 shrink-0 items-center justify-center rounded-md border border-border bg-muted/50 text-primary">
                      <Icon className="size-4" />
                    </span>
                    <span className="flex min-w-0 flex-col gap-1">
                      <span className="font-medium leading-snug">{module.name}</span>
                      <span className="text-muted-foreground text-sm leading-snug">
                        {module.blurb}
                      </span>
                      <span className="text-sm font-semibold text-primary group-hover:underline">
                        Open →
                      </span>
                    </span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </CardContent>
      </Card>
    </main>
  );
}
