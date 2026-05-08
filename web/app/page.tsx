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
    <main className="mx-auto flex w-full max-w-none flex-1 flex-col gap-8 px-6 py-12">
      <header className="flex flex-col gap-3">
        <p className="text-muted-foreground text-sm font-medium uppercase">
          Software supply chain analysis
        </p>
        <h1 className="max-w-3xl text-3xl font-semibold sm:text-4xl">
          Evaluate dependency risk across package ecosystems.
        </h1>
        <p className="text-muted-foreground max-w-3xl text-lg leading-7">
          Keep an eye on the dependencies that matter, compare package risk,
          inspect dependency paths, and return to saved watch lists.
        </p>
      </header>

      {!user && (
        <Card className="max-w-3xl">
          <CardHeader>
            <CardTitle>Sign in to track dependencies</CardTitle>
            <CardDescription>
              Google and GitHub login unlock saved package tracking and repository imports.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-3">
            <Button onClick={() => login("google")}>
              <Mail />
              Continue with Gmail
            </Button>
            <Button variant="outline" onClick={() => login("github")}>
              Continue with GitHub
            </Button>
          </CardContent>
        </Card>
      )}

      {user && (
        <Card className="max-w-3xl">
          <CardHeader>
            <CardTitle>
              Signed in as {user.displayName || user.email || "your account"}
            </CardTitle>
            <CardDescription>
              Saved dependencies and GitHub imports are available from the tracking page.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-3">
            <Button asChild>
              <Link href="/track">
                <Bell />
                Track dependencies
              </Link>
            </Button>
            <Button asChild variant="outline">
              <Link href="/packages">Browse packages</Link>
            </Button>
          </CardContent>
        </Card>
      )}

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {signals.map((signal) => (
          <Card key={signal.label}>
            <CardHeader>
              <CardTitle className="text-base">{signal.label}</CardTitle>
              <CardDescription>{signal.value}</CardDescription>
            </CardHeader>
          </Card>
        ))}
      </div>

      <Card className="max-w-3xl">
        <CardHeader>
          <CardTitle>Start exploring</CardTitle>
          <CardDescription>
            Main database-backed views for package risk, graph structure, and saved tracking.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ul className="flex flex-col gap-3">
            {modules.map((module) => {
              const Icon = module.icon;
              return (
                <li key={module.name}>
                  <Link
                    href={module.href}
                    className="group flex gap-3 rounded-md border border-border p-3 transition-colors hover:bg-accent hover:text-accent-foreground"
                  >
                    <span className="mt-0.5 text-muted-foreground group-hover:text-accent-foreground">
                      <Icon className="size-4" />
                    </span>
                    <span className="flex flex-col gap-0.5">
                      <span className="font-medium">{module.name}</span>
                      <span className="text-muted-foreground text-sm">
                        {module.blurb}
                      </span>
                      <span className="text-sm font-medium text-primary">Open</span>
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
