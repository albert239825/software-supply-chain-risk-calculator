"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Bell, Mail, Plus, Search, Trash2 } from "lucide-react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { PageSpinner } from "@/components/ui/spinner";

type User = {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
};

type SearchRow = {
  package_id: string;
  package_name: string;
  ecosystem: string;
  latest_version: string;
};

type TrackedRow = {
  id: string;
  package_id: string;
  note: string | null;
  created_at: string;
  updated_at: string;
  package_name: string;
  ecosystem: string;
  description: string | null;
  latest_version: string;
  latest_version_id: string | null;
};

type GitHubRepo = {
  id: number;
  name: string;
  fullName: string;
  private: boolean;
  defaultBranch: string;
  htmlUrl: string;
  updatedAt: string;
};

type GitHubImportResult = {
  repo: string;
  total: number;
  matched: Array<{
    package_id: string;
    package_name: string;
    ecosystem: string;
    latest_version: string;
    latest_version_id: string | null;
  }>;
  unmatched: string[];
};

export default function TrackPage() {
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [tracked, setTracked] = useState<TrackedRow[]>([]);
  const [trackedLoading, setTrackedLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchRow[]>([]);
  const [searching, setSearching] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [repos, setRepos] = useState<GitHubRepo[]>([]);
  const [selectedRepo, setSelectedRepo] = useState("");
  const [reposLoading, setReposLoading] = useState(false);
  const [importingRepo, setImportingRepo] = useState(false);
  const [importResult, setImportResult] = useState<GitHubImportResult | null>(null);

  const trackedIds = useMemo(
    () => new Set(tracked.map((row) => row.package_id)),
    [tracked],
  );

  useEffect(() => {
    fetch("/api/auth/me")
      .then((res) => res.json())
      .then((body: { user: User | null }) => {
        setUser(body.user);
        setAuthLoading(false);
      })
      .catch((error: Error) => {
        setMessage(error.message);
        setAuthLoading(false);
      });
  }, []);

  const refreshTracked = useCallback(async () => {
    setTrackedLoading(true);
    const res = await fetch("/api/tracked-dependencies");
    const body = await res.json();
    if (!res.ok) {
      setMessage(body.error || "Could not load tracked dependencies");
      setTrackedLoading(false);
      return;
    }
    setTracked(body);
    setTrackedLoading(false);
  }, []);

  useEffect(() => {
    if (!user) {
      return;
    }

    fetch("/api/tracked-dependencies")
      .then((res) => res.json())
      .then((body: TrackedRow[] | { error: string }) => {
        if ("error" in body) {
          setMessage(body.error);
          return;
        }
        setTracked(body);
      })
      .catch((error: Error) => setMessage(error.message));
  }, [user]);

  useEffect(() => {
    if (query.trim().length < 2) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setSearching(true);
      fetch(`/api/graph/search?q=${encodeURIComponent(query)}`)
        .then((res) => res.json())
        .then((rows: SearchRow[]) => {
          setResults(rows);
          setSearching(false);
        })
        .catch((error: Error) => {
          setMessage(error.message);
          setSearching(false);
        });
    }, 250);

    return () => window.clearTimeout(timeout);
  }, [query]);

  function updateQuery(value: string) {
    setQuery(value);
    if (value.trim().length < 2) {
      setResults([]);
    }
  }

  function login(provider: "google" | "github") {
    window.location.href = `/api/auth/${provider}`;
  }

  async function trackPackage(packageId: string) {
    const res = await fetch("/api/tracked-dependencies", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ packageId }),
    });
    const body = await res.json();
    if (!res.ok) {
      setMessage(body.error || "Could not track package");
      return;
    }
    setMessage("Dependency added.");
    await refreshTracked();
  }

  async function removePackage(packageId: string) {
    const res = await fetch(`/api/tracked-dependencies/${packageId}`, {
      method: "DELETE",
    });
    const body = await res.json();
    if (!res.ok) {
      setMessage(body.error || "Could not remove package");
      return;
    }
    setTracked((rows) => rows.filter((row) => row.package_id !== packageId));
  }

  async function loadRepos() {
    setReposLoading(true);
    setMessage(null);
    const res = await fetch("/api/github/repos");
    const body = await res.json();
    if (!res.ok) {
      setMessage(body.error || "Could not load GitHub repositories");
      setReposLoading(false);
      return;
    }

    setRepos(body);
    setSelectedRepo(body[0]?.fullName ?? "");
    setReposLoading(false);
  }

  async function importRepoDependencies() {
    if (!selectedRepo) {
      return;
    }

    setImportingRepo(true);
    setMessage(null);
    const res = await fetch("/api/github/repos/import", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ fullName: selectedRepo }),
    });
    const body = await res.json();
    if (!res.ok) {
      setMessage(body.error || "Could not import GitHub dependencies");
      setImportingRepo(false);
      return;
    }

    setImportResult(body);
    await refreshTracked();
    setImportingRepo(false);
  }

  if (authLoading) {
    return (
      <main className="mx-auto max-w-3xl px-6 py-20">
        <div className="rounded-lg border border-border bg-card p-8 shadow-sm">
          <PageSpinner label="Checking account..." />
        </div>
      </main>
    );
  }

  if (!user) {
    return (
      <main className="mx-auto grid w-full max-w-6xl flex-1 gap-6 px-6 pb-16 pt-10 lg:grid-cols-[1fr_380px] lg:items-start">
        <section className="flex flex-col gap-6">
          <div className="inline-flex w-fit items-center gap-2 rounded-md border border-border bg-card px-3 py-1 text-sm font-medium text-muted-foreground">
            <Bell className="size-4" />
            Personal watch lists
          </div>
          <div className="flex flex-col gap-4">
            <h1 className="max-w-3xl text-4xl font-semibold tracking-tight sm:text-5xl">
              Save the dependencies you want to keep an eye on.
            </h1>
            <p className="max-w-2xl text-lg leading-8 text-muted-foreground">
              Sign in with Gmail or GitHub, then build a package watch list for
              future historical checks, alerts, and risk changes.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-3">
            {[
              ["Search", "Find packages already loaded in the DB."],
              ["Save", "Attach packages to your account."],
              ["Review", "Return to the same list later."],
            ].map(([title, copy]) => (
              <div key={title} className="rounded-md border border-border bg-card p-4 shadow-sm">
                <p className="font-semibold">{title}</p>
                <p className="mt-1 text-sm leading-6 text-muted-foreground">{copy}</p>
              </div>
            ))}
          </div>
        </section>

        <Card className="border-border bg-card shadow-sm">
          <CardHeader>
            <CardTitle className="text-2xl">Log in</CardTitle>
            <CardDescription className="leading-6">
              Use your Gmail or GitHub account to save tracked dependencies.
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
      </main>
    );
  }

  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-6 px-6 pb-16 pt-10">
      <header className="rounded-lg border border-border bg-card p-6 shadow-sm">
        <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-end">
          <div className="flex flex-col gap-2">
            <div className="inline-flex w-fit items-center gap-2 rounded-md bg-secondary px-3 py-1 text-sm font-semibold text-secondary-foreground">
              <Bell className="size-4" />
              {tracked.length} tracked
            </div>
            <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
              Tracked dependencies
            </h1>
            <p className="text-muted-foreground">
              Signed in as {user.displayName || user.email || "your account"}.
            </p>
          </div>
          <div className="rounded-md border border-border bg-muted/50 px-4 py-3 text-sm text-muted-foreground">
            Watch-list changes are saved to your account.
          </div>
        </div>
      </header>

      <Card className="border-border bg-card shadow-sm">
        <CardHeader>
          <CardTitle>Import from GitHub</CardTitle>
          <CardDescription>
            Load a repository package.json and track dependencies that exist in this app.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4">
          <div className="flex flex-col gap-3 md:flex-row">
            <Button
              type="button"
              variant="outline"
              onClick={loadRepos}
              disabled={reposLoading}
            >
              {reposLoading ? "Loading repos..." : "Load GitHub repos"}
            </Button>
            <select
              value={selectedRepo}
              onChange={(event) => setSelectedRepo(event.target.value)}
              className="h-9 min-w-0 flex-1 rounded-lg border border-input bg-background px-3 text-sm"
              disabled={repos.length === 0}
            >
              {repos.length === 0 ? (
                <option value="">No repositories loaded</option>
              ) : (
                repos.map((repo) => (
                  <option key={repo.id} value={repo.fullName}>
                    {repo.fullName}
                    {repo.private ? " (private)" : ""}
                  </option>
                ))
              )}
            </select>
            <Button
              type="button"
              onClick={importRepoDependencies}
              disabled={!selectedRepo || importingRepo}
            >
              {importingRepo ? "Importing..." : "Import dependencies"}
            </Button>
          </div>

          {importResult && (
            <div className="rounded-md border border-border bg-muted/40 p-4 text-sm">
              <p className="font-medium">
                Imported {importResult.matched.length} of {importResult.total} dependencies from{" "}
                {importResult.repo}.
              </p>
              {importResult.unmatched.length > 0 && (
                <p className="mt-1 text-muted-foreground">
                  {importResult.unmatched.length} dependencies were not in the local package database.
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <Card className="border-border bg-card shadow-sm">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="size-5 text-primary" />
            Add a dependency
          </CardTitle>
          <CardDescription>Search packages already loaded in the risk database.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Input
            value={query}
            onChange={(event) => updateQuery(event.target.value)}
            placeholder="Search package names"
            className="h-11 bg-background"
          />
          {searching && <PageSpinner label="Searching..." />}
          <div className="grid gap-3 md:grid-cols-2">
            {results.map((row) => (
              <div
                key={row.package_id}
                className="flex items-center justify-between gap-4 rounded-md border border-border bg-background p-4 shadow-sm"
              >
                <div className="min-w-0">
                  <p className="truncate font-medium">{row.package_name}</p>
                  <p className="text-muted-foreground text-sm">
                    {row.ecosystem} &middot; {row.latest_version}
                  </p>
                </div>
                <Button
                  size="sm"
                  variant={trackedIds.has(row.package_id) ? "secondary" : "default"}
                  disabled={trackedIds.has(row.package_id)}
                  onClick={() => trackPackage(row.package_id)}
                >
                  <Plus />
                  {trackedIds.has(row.package_id) ? "Tracked" : "Track"}
                </Button>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card shadow-sm">
        <CardHeader>
          <CardTitle>Your watch list</CardTitle>
          <CardDescription>
            Packages saved here can be used for historical checks and alerts later.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {message && <p className="mb-4 text-sm text-muted-foreground">{message}</p>}
          {trackedLoading && <PageSpinner label="Loading tracked dependencies..." />}
          {!trackedLoading && tracked.length === 0 && (
            <div className="rounded-md border border-dashed border-border bg-muted/50 p-6 text-foreground">
              <p className="font-semibold">No tracked dependencies yet.</p>
              <p className="mt-1 text-sm leading-6">
                Search above and add packages that deserve a closer look.
              </p>
            </div>
          )}
          <div className="grid gap-3 md:grid-cols-2">
            {tracked.map((row) => (
              <div
                key={row.id}
                className="flex items-start justify-between gap-4 rounded-md border border-border bg-background p-4 shadow-sm"
              >
                <div className="min-w-0">
                  <p className="truncate text-lg font-semibold">{row.package_name}</p>
                  <p className="text-muted-foreground text-sm">
                    {row.ecosystem} &middot; {row.latest_version} &middot; added{" "}
                    {row.created_at.slice(0, 10)}
                  </p>
                  {row.description && (
                    <p className="text-muted-foreground mt-1 line-clamp-2 text-sm">
                      {row.description}
                    </p>
                  )}
                  {row.latest_version_id && (
                    <Link
                      href={`/graph?versionId=${encodeURIComponent(row.latest_version_id)}&packageId=${encodeURIComponent(row.package_id)}`}
                      className="mt-2 inline-block text-sm font-semibold text-primary hover:underline"
                    >
                      Open graph
                    </Link>
                  )}
                </div>
                <Button
                  aria-label={`Remove ${row.package_name}`}
                  title={`Remove ${row.package_name}`}
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => removePackage(row.package_id)}
                >
                  <Trash2 />
                </Button>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
