"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface PackageRow {
  package_id?: string;
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
      .then(async (res) => {
        if (res.ok === false) throw new Error("Could not load packages");
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
        <h1 className="text-2xl font-semibold">Package versions</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm">
          A release-ordered view of packages and versions in the database.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Latest collected versions</CardTitle>
          <CardDescription>
            Sorted by release date. Package ids open detail pages when available.
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
                    <th>Released</th>
                  </tr>
                </thead>
                <tbody>
                  {data.slice(0, 200).map((row, i) => (
                    <tr key={row.package_name + row.version + i}>
                      <td className="font-medium">
                        {row.package_id ? (
                          <Link className="hover:underline" href={`/packages/${row.package_id}`}>
                            {row.package_name}
                          </Link>
                        ) : (
                          row.package_name
                        )}
                      </td>
                      <td className="font-mono text-xs">{row.version || "Unknown"}</td>
                      <td>{row.released?.slice(0, 10) ?? "Unknown"}</td>
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
