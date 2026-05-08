"use client";
import { useEffect, useState } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface AbandonedRow {
  package_name: string;
  dependents: number;
  last_release: string;
}

export default function AbandonedPage() {
  const [data, setData] = useState<AbandonedRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/risk/abandoned-popular")
      .then(async (res) => {
        if (res.ok === false) throw new Error("Could not load abandoned packages");
        return res.json();
      })
      .then((rows) => {
        setData(rows);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  return (
    <main className="mx-auto max-w-5xl space-y-6 px-6 py-12">
      <header>
        <h1 className="text-2xl font-semibold">Abandoned but popular</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm">
          Packages with stale releases that still have meaningful dependent usage.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>High-impact stale packages</CardTitle>
          <CardDescription>
            Ranked by dependent count, with oldest release dates surfaced for review.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading abandoned packages…" />}
          {error && <div className="text-destructive text-sm">{error}</div>}
          {!loading && !error && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Package</th>
                    <th className="text-right">Dependents</th>
                    <th>Last release</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, i) => (
                    <tr key={row.package_name + row.last_release + row.dependents + i}>
                      <td className="font-medium">{row.package_name}</td>
                      <td className="text-right tabular-nums">
                        {Number(row.dependents).toLocaleString()}
                      </td>
                      <td>{row.last_release?.slice(0, 10) ?? "Unknown"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
