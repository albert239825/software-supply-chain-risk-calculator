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
      .then(async (res) => {
        if (res.ok === false) throw new Error("Could not load maintainers");
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
    <main className="mx-auto max-w-4xl space-y-6 px-6 py-12">
      <header>
        <h1 className="text-2xl font-semibold">Maintainers</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm">
          Maintainers responsible for the largest number of packages in the dataset.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Top maintainers</CardTitle>
          <CardDescription>
            Concentration of package ownership can be a supply-chain risk signal.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading maintainers…" />}
          {error && <div className="text-destructive text-sm">{error}</div>}
          {!loading && !error && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Username</th>
                    <th className="text-right">Packages</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row) => (
                    <tr key={row.username}>
                      <td className="font-medium">{row.username || "Unknown"}</td>
                      <td className="text-right tabular-nums">
                        {Number(row.num_packages).toLocaleString()}
                      </td>
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
