"use client";
import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface MaintainerRow {
  username: string;
  num_packages: number;
}

export default function MaintainersPage() {
  const [data, setData] = useState<MaintainerRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/maintainers/top")
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
          <CardTitle>Top Maintainers</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading maintainers…" />}
          {error && <div className="text-red-500">{error}</div>}
          {!loading && !error && (
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Num Packages</th>
                </tr>
              </thead>
              <tbody>
                {data.map((row) => (
                  <tr key={row.username}>
                    <td>{row.username}</td>
                    <td>{row.num_packages}</td>
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
