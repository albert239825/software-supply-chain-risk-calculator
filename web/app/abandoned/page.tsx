"use client";
import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
      .then((res) => res.json())
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
    <main className="mx-auto max-w-3xl px-6 py-16">
      <Card>
        <CardHeader>
          <CardTitle>Abandoned but Popular Packages</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading abandoned packages…" />}
          {error && <div className="text-red-500">{error}</div>}
          {!loading && !error && (
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  <th>Package</th>
                  <th>Dependents</th>
                  <th>Last Release</th>
                </tr>
              </thead>
              <tbody>
                {data.map((row, i) => (
                  <tr key={row.package_name + row.last_release + row.dependents + i}>
                    <td>{row.package_name}</td>
                    <td>{row.dependents}</td>
                    <td>{row.last_release?.slice(0, 10)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
