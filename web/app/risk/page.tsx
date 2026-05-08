"use client";

import { useCallback, useEffect, useState } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PageSpinner, Spinner } from "@/components/ui/spinner";

interface RiskRow {
  package_id: string;
  package_name: string;
  maintainers: number;
  dependencies: number;
  last_release: string | null;
  risk_score: number;
  bucket: string;
}

const PAGE_LIMIT = 30;

type RankedResponse =
  | {
      items: RiskRow[];
      total: number;
      limit: number;
      offset: number;
      hasMore: boolean;
    }
  | { error?: string };

function bucketClass(bucket: string) {
  if (bucket === "high") {
    return "border-red-200 bg-red-50 text-red-800";
  }
  if (bucket === "medium") {
    return "border-yellow-200 bg-yellow-50 text-yellow-800";
  }
  return "border-green-200 bg-green-50 text-green-800";
}

function rowClass(bucket: string) {
  if (bucket === "high") {
    return "bg-red-50/35 hover:bg-red-50/65";
  }
  if (bucket === "medium") {
    return "bg-yellow-50/35 hover:bg-yellow-50/65";
  }
  return "bg-green-50/25 hover:bg-green-50/55";
}

export default function RiskAnalysisPage() {
  const [data, setData] = useState<RiskRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [total, setTotal] = useState<number | null>(null);
  const [hasMore, setHasMore] = useState(false);

  const loadPage = useCallback(async (offset: number, append: boolean) => {
    if (append) setLoadingMore(true);
    else {
      setLoading(true);
      setError(null);
    }

    try {
      const qs = new URLSearchParams({
        limit: String(PAGE_LIMIT),
        offset: String(offset),
      });

      const res = await fetch(`/api/risk/ranked?${qs.toString()}`);
      const payload: unknown = await res.json();

      if (!res.ok) {
        throw new Error(
          payload !== null && typeof payload === "object" && "error" in payload
            ? String((payload as { error?: string }).error)
            : `Request failed (${res.status})`,
        );
      }

      if (
        payload === null ||
        typeof payload !== "object" ||
        !Array.isArray((payload as RankedResponse & { items?: unknown }).items)
      ) {
        throw new Error("Unexpected response shape from risk API");
      }

      const body = payload as {
        items: RiskRow[];
        total: number;
        hasMore: boolean;
      };

      setTotal(body.total);
      setHasMore(body.hasMore);

      if (append) {
        setData((prev) => [...prev, ...body.items]);
      } else {
        setData(body.items);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      if (!append) setData([]);
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => void loadPage(0, false), 0);
    return () => window.clearTimeout(timer);
  }, [loadPage]);

  return (
    <main className="mx-auto max-w-3xl px-6 py-16">
      <Card>
        <CardHeader>
          <CardTitle>Top risky packages</CardTitle>
          <CardDescription>
            Sorted by composite risk score (high first). Use “Load more” for the
            next page.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          {loading && <PageSpinner label="Loading rankings…" />}
          {error && (
            <div className="text-destructive text-sm">{error}</div>
          )}

          {!loading && !error && (
            <>
              {total !== null ? (
                <p className="text-muted-foreground text-xs tabular-nums">
                  Showing {data.length.toLocaleString()} of {total.toLocaleString()}{" "}
                  ranked packages ({PAGE_LIMIT} per request).
                </p>
              ) : null}

              <div className="overflow-x-auto rounded-lg border">
                <table className="w-full border-collapse text-left text-sm">
                  <thead className="bg-muted/60">
                    <tr>
                      <th className="border-b px-3 py-2 font-medium">
                        Package
                      </th>
                      <th className="border-b px-3 py-2 font-medium tabular-nums">
                        Maintainers
                      </th>
                      <th className="border-b px-3 py-2 font-medium tabular-nums">
                        Direct deps
                      </th>
                      <th className="border-b px-3 py-2 font-medium">
                        Last release
                      </th>
                      <th className="border-b px-3 py-2 font-medium tabular-nums">
                        Risk
                      </th>
                      <th className="border-b px-3 py-2 font-medium">Bucket</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.map((row) => (
                      <tr key={row.package_id} className={rowClass(row.bucket)}>
                        <td className="border-b px-3 py-2">{row.package_name}</td>

                        <td className="border-b px-3 py-2 tabular-nums">
                          {row.maintainers}
                        </td>

                        <td className="border-b px-3 py-2 tabular-nums">
                          {row.dependencies}
                        </td>

                        <td className="border-b px-3 py-2">
                          {row.last_release?.slice(0, 10) ?? "—"}
                        </td>

                        <td className="border-b px-3 py-2 tabular-nums font-medium">
                          {Number(row.risk_score).toFixed(2)}
                        </td>

                        <td className="border-b px-3 py-2 capitalize">
                          <span className={`inline-flex rounded-md border px-2 py-0.5 text-xs font-semibold ${bucketClass(row.bucket)}`}>
                            {row.bucket}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {!data.length ? (
                <p className="text-muted-foreground text-sm">
                  No packages returned.
                </p>
              ) : null}

              {hasMore ? (
                <Button
                  type="button"
                  variant="outline"
                  className="w-full sm:w-auto"
                  disabled={loadingMore}
                  onClick={() => void loadPage(data.length, true)}
                >
                  {loadingMore ? (
                    <span className="inline-flex items-center justify-center gap-2">
                      <Spinner size="sm" className="text-primary" />
                      Loading…
                    </span>
                  ) : (
                    "Load more"
                  )}
                </Button>
              ) : null}
            </>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
