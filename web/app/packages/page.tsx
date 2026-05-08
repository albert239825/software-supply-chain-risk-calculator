"use client";
import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface PackageRow {
  package_name: string;
  version: string;
  released: string;
}

export default function PackagesPage() {
  const [data, setData] = useState<PackageRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/packages/all")
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
          <CardTitle>All Packages</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading packages…" />}
          {error && <div className="text-red-500">{error}</div>}
          {!loading && !error && (
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  <th>Package</th>
                  <th>Version</th>
                  <th>Released</th>
                </tr>
              </thead>
              <tbody>
                {data.map((row, i) => (
                  <tr key={row.package_name + row.version + i}>
                    <td>{row.package_name}</td>
                    <td>{row.version}</td>
                    <td>{row.released?.slice(0, 10)}</td>
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
