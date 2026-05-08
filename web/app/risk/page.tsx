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
    return "border-amber-200 bg-amber-50 text-amber-800";
  }
  return "border-emerald-200 bg-emerald-50 text-emerald-800";
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
    <main className="mx-auto w-full max-w-none px-6 py-12">
      <Card>
        <CardHeader>
          <CardTitle>Top risky packages</CardTitle>
          <CardDescription>
            Sorted by composite risk score across maintainers, staleness,
            dependency fan-out, dependent count, and repository metadata.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          {loading && <PageSpinner label="Loading rankings..." />}
          {error && <div className="text-destructive text-sm">{error}</div>}

          {!loading && !error && (
            <>
              {total !== null ? (
                <p className="text-muted-foreground text-xs tabular-nums">
                  Showing {data.length.toLocaleString()} of {total.toLocaleString()}{" "}
                  ranked packages ({PAGE_LIMIT} per request).
                </p>
              ) : null}

              <div className="table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Package</th>
                      <th className="text-right tabular-nums">Maintainers</th>
                      <th className="text-right tabular-nums">Direct deps</th>
                      <th>Last release</th>
                      <th className="text-right tabular-nums">Risk</th>
                      <th>Bucket</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.map((row) => (
                      <tr key={row.package_id}>
                        <td className="font-medium">{row.package_name}</td>
                        <td className="text-right tabular-nums">
                          {Number(row.maintainers).toLocaleString()}
                        </td>
                        <td className="text-right tabular-nums">
                          {Number(row.dependencies).toLocaleString()}
                        </td>
                        <td>{row.last_release?.slice(0, 10) ?? "-"}</td>
                        <td className="text-right tabular-nums font-medium">
                          {Number(row.risk_score).toFixed(2)}
                        </td>
                        <td className="capitalize">
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
                      Loading...
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
