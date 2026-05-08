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

interface NoRepoRow {
  package_name: string;
  version: string;
}

export default function NoRepoPage() {
  const [data, setData] = useState<NoRepoRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/packages/no-repo")
      .then(async (res) => {
        if (res.ok === false) throw new Error("Could not load packages without repositories");
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
    <main className="mx-auto w-full max-w-none space-y-6 px-6 py-12">
      <header>
        <h1 className="text-2xl font-semibold">Packages without repositories</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm">
          Versions with no repository metadata, which makes auditing harder.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Missing repository links</CardTitle>
          <CardDescription>
            These rows are useful for the repository-presence risk signal.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading && <PageSpinner label="Loading packages…" />}
          {error && <div className="text-destructive text-sm">{error}</div>}
          {!loading && !error && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Package</th>
                    <th>Version</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, i) => (
                    <tr key={row.package_name + row.version + i}>
                      <td className="font-medium">{row.package_name}</td>
                      <td className="font-mono text-xs">{row.version || "Unknown"}</td>
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
