"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  Activity,
  Bell,
  GitBranch,
  Mail,
  PackageSearch,
  Search,
} from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PageSpinner } from "@/components/ui/spinner";

type User = {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
};

const modules = [
  {
    name: "Packages",
    blurb: "Browse package records.",
    href: "/packages",
    icon: PackageSearch,
  },
  {
    name: "Risk",
    blurb: "Compare risk scores.",
    href: "/risk",
    icon: Activity,
  },
  {
    name: "Graph",
    blurb: "Explore dependency paths.",
    href: "/graph",
    icon: GitBranch,
  },
  {
    name: "Tracked",
    blurb: "Manage your watch list.",
    href: "/track",
    icon: Bell,
  },
];

export default function Home() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/auth/me")
      .then((res) => res.json())
      .then((body: { user: User | null }) => {
        setUser(body.user);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  function login(provider: "google" | "github") {
    window.location.href = `/api/auth/${provider}`;
  }

  if (loading) {
    return (
      <main className="mx-auto flex w-full max-w-3xl flex-1 items-center px-6 py-16">
        <Card className="w-full border-border bg-card shadow-sm">
          <CardContent className="p-8">
            <PageSpinner label="Checking account..." />
          </CardContent>
        </Card>
      </main>
    );
  }

  if (!user) {
    return (
      <main className="mx-auto flex w-full max-w-5xl flex-1 items-center px-6 pb-16 pt-10">
        <section className="grid w-full gap-6 lg:grid-cols-[1fr_360px] lg:items-center">
          <div className="flex flex-col gap-3">
            <p className="text-sm font-medium text-muted-foreground">
              Software Supply Chain Risk Scorer
            </p>
            <h1 className="max-w-2xl text-4xl font-semibold tracking-tight sm:text-5xl">
              Keep an eye on the dependencies that matter.
            </h1>
            <p className="max-w-xl text-lg leading-8 text-muted-foreground">
              Sign in to save packages, import from GitHub, and return to your
              watch list anytime.
            </p>
          </div>

          <Card className="border-border bg-card shadow-sm">
            <CardHeader>
              <CardTitle className="text-2xl">Log in</CardTitle>
              <CardDescription>
                Choose how you want to continue.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3">
              <Button size="lg" onClick={() => login("google")}>
                <Mail />
                Continue with Gmail
              </Button>
              <Button size="lg" variant="outline" onClick={() => login("github")}>
                Continue with GitHub
              </Button>
            </CardContent>
          </Card>
        </section>
      </main>
    );
  }

  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-6 px-6 pb-16 pt-10">
      <section className="rounded-lg border border-border bg-card p-6 shadow-sm">
        <div className="flex flex-col justify-between gap-5 md:flex-row md:items-end">
          <div className="flex flex-col gap-2">
            <p className="text-sm font-medium text-muted-foreground">
              Signed in as {user.displayName || user.email || "your account"}
            </p>
            <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
              Dependency risk dashboard
            </h1>
          </div>
          <div className="flex flex-wrap gap-3">
            <Button asChild>
              <Link href="/track">
                <Bell />
                Track dependencies
              </Link>
            </Button>
            <Button asChild variant="outline">
              <Link href="/packages">
                <Search />
                Browse packages
              </Link>
            </Button>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {modules.map((module) => {
          const Icon = module.icon;
          return (
            <Link key={module.name} href={module.href} className="group">
              <Card className="h-full border-border bg-card shadow-sm transition-colors hover:border-primary/40">
                <CardHeader>
                  <div className="mb-3 flex size-10 items-center justify-center rounded-md border border-border bg-muted/50 text-primary">
                    <Icon className="size-5" />
                  </div>
                  <CardTitle className="text-lg">{module.name}</CardTitle>
                  <CardDescription className="leading-6">
                    {module.blurb}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <span className="text-sm font-semibold text-primary group-hover:underline">
                    Open
                  </span>
                </CardContent>
              </Card>
            </Link>
          );
        })}
      </section>
    </main>
  );
}
