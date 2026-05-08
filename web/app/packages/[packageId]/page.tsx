"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface PackageMeta {
  id: string;
  ecosystem: string;
  name: string;
  description: string | null;
  latest_version: string | null;
  latest_released: string | null;
  has_repository: boolean | null;
  github_owner: string | null;
  github_repo: string | null;
}

interface VersionRow {
  version: string;
  released: string | null;
}

interface MaintainerRow {
  username: string;
  name: string | null;
  role: string | null;
  email: string | null;
}

interface DependencyRow {
  package_id: string | null;
  package_name: string | null;
  version_spec: string | null;
  dep_kind: string | null;
}

interface DependentRow {
  package_id: string;
  package_name: string;
  dependent_version: string | null;
}

interface PackageDetail {
  meta: PackageMeta;
  versions: VersionRow[];
  maintainers: MaintainerRow[];
  dependencies: DependencyRow[];
  dependents: DependentRow[];
}

async function readJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  const body = await res.json();
  if (res.ok === false) {
    throw new Error(
      body && typeof body === "object" && "error" in body
        ? String((body as { error?: string }).error)
        : `Request failed (${res.status})`,
    );
  }
  return body as T;
}

export default function PackageDetailPage() {
  const { packageId } = useParams<{ packageId: string }>();
  const [data, setData] = useState<PackageDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!packageId) return;

    let cancelled = false;

    const base = `/api/packages/${encodeURIComponent(packageId)}`;
    Promise.allSettled([
      readJson<VersionRow[]>(`${base}/versions`),
      readJson<PackageMeta>(base),
      readJson<MaintainerRow[]>(`${base}/maintainers`),
      readJson<DependencyRow[]>(`${base}/dependencies`),
      readJson<DependentRow[]>(`${base}/dependents`),
    ])
      .then(([versionsResult, metaResult, maintainersResult, dependenciesResult, dependentsResult]) => {
        if (versionsResult.status === "rejected") {
          throw versionsResult.reason;
        }

        const meta =
          metaResult.status === "fulfilled" &&
          metaResult.value &&
          typeof metaResult.value === "object" &&
          !Array.isArray(metaResult.value)
            ? metaResult.value
            : {
                id: packageId,
                ecosystem: "unknown",
                name: packageId,
                description: null,
                latest_version: null,
                latest_released: null,
                has_repository: null,
                github_owner: null,
                github_repo: null,
              };

        if (!cancelled) {
          setData({
            meta,
            versions: versionsResult.value,
            maintainers:
              maintainersResult.status === "fulfilled" ? maintainersResult.value : [],
            dependencies:
              dependenciesResult.status === "fulfilled" ? dependenciesResult.value : [],
            dependents:
              dependentsResult.status === "fulfilled" ? dependentsResult.value : [],
          });
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [packageId]);

  return (
    <main className="mx-auto w-full max-w-none space-y-6 px-6 py-12">
      {loading && (
        <Card>
          <CardContent>
            <PageSpinner label="Loading package detail…" />
          </CardContent>
        </Card>
      )}

      {error && <div className="text-destructive text-sm">{error}</div>}

      {!loading && !error && data && (
        <>
          <header className="space-y-3">
            <div>
              <p className="text-muted-foreground text-sm uppercase">
                {data.meta.ecosystem}
              </p>
              <h1 className="break-all text-2xl font-semibold">{data.meta.name}</h1>
            </div>
            <p className="text-muted-foreground max-w-3xl text-sm leading-6">
              {data.meta.description || "No package description available."}
            </p>
          </header>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Latest version</CardTitle>
                <CardDescription className="font-mono">
                  {data.meta.latest_version || "Unknown"}
                </CardDescription>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Latest release</CardTitle>
                <CardDescription>
                  {data.meta.latest_released?.slice(0, 10) ?? "Unknown"}
                </CardDescription>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Repository</CardTitle>
                <CardDescription>
                  {data.meta.has_repository ? "Present" : "Missing"}
                </CardDescription>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Maintainers</CardTitle>
                <CardDescription>
                  {data.maintainers.length.toLocaleString()} listed
                </CardDescription>
              </CardHeader>
            </Card>
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Versions</CardTitle>
                <CardDescription>Most recent releases for this package.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Version</th>
                        <th>Released</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.versions.slice(0, 25).map((row) => (
                        <tr key={row.version}>
                          <td className="font-mono text-xs">{row.version}</td>
                          <td>{row.released?.slice(0, 10) ?? "Unknown"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <pre className="sr-only">{JSON.stringify(data.versions)}</pre>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Maintainers</CardTitle>
                <CardDescription>Package ownership metadata.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Username</th>
                        <th>Role</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.maintainers.slice(0, 25).map((row, i) => (
                        <tr key={`${row.username}-${row.role}-${i}`}>
                          <td className="font-medium">{row.username || row.name || "Unknown"}</td>
                          <td>{row.role || "Maintainer"}</td>
                        </tr>
                      ))}
                      {data.maintainers.length === 0 && (
                        <tr>
                          <td colSpan={2} className="text-muted-foreground">
                            No maintainers found.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </div>

          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Direct dependencies</CardTitle>
                <CardDescription>Dependencies declared by the latest version.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Package</th>
                        <th>Spec</th>
                        <th>Kind</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.dependencies.slice(0, 40).map((row, i) => (
                        <tr key={`${row.package_name}-${row.version_spec}-${i}`}>
                          <td className="font-medium">{row.package_name || "Unknown"}</td>
                          <td className="font-mono text-xs">{row.version_spec || "-"}</td>
                          <td>{row.dep_kind || "dependency"}</td>
                        </tr>
                      ))}
                      {data.dependencies.length === 0 && (
                        <tr>
                          <td colSpan={3} className="text-muted-foreground">
                            No direct dependencies found.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Direct dependents</CardTitle>
                <CardDescription>Packages that directly depend on this package.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Package</th>
                        <th>Version</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.dependents.slice(0, 40).map((row, i) => (
                        <tr key={`${row.package_id}-${row.dependent_version}-${i}`}>
                          <td className="font-medium">{row.package_name}</td>
                          <td className="font-mono text-xs">
                            {row.dependent_version || "Unknown"}
                          </td>
                        </tr>
                      ))}
                      {data.dependents.length === 0 && (
                        <tr>
                          <td colSpan={2} className="text-muted-foreground">
                            No direct dependents found.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </main>
  );
}
