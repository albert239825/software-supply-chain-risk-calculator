"use client";

import { useEffect, useState } from "react";

import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { PageSpinner } from "@/components/ui/spinner";

interface FanoutRow {
  package_name: string;
  num_dependencies: number;
}

interface DependentsRow {
  package_name: string;
  dependents: number;
}

export default function StatsPage() {
  const [fanout, setFanout] = useState<FanoutRow[]>([]);
  const [dependents, setDependents] = useState<DependentsRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      fetch("/api/stats/top-fanout").then((res) => res.json()),
      fetch("/api/stats/most-dependents").then((res) => res.json()),
    ])
      .then(([fanoutRows, dependentsRows]) => {
        setFanout(fanoutRows);
        setDependents(dependentsRows);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  return (
    <main className="mx-auto max-w-3xl space-y-8 px-6 py-16">
      {loading ? (
        <Card>
          <CardContent>
            <PageSpinner label="Loading statistics…" />
          </CardContent>
        </Card>
      ) : (
        <>
          {error ? (
            <div className="text-destructive text-sm">{error}</div>
          ) : (
            <>
              <Card>
                <CardHeader>
                  <CardTitle>Top Packages by Dependency Count</CardTitle>
                </CardHeader>
                <CardContent>
                  <table className="mb-8 w-full border-collapse text-left">
                    <thead>
                      <tr>
                        <th>Package</th>
                        <th>Num Dependencies</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fanout.map((row) => (
                        <tr key={row.package_name}>
                          <td>{row.package_name}</td>
                          <td>{row.num_dependencies}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Most Depended-on Packages</CardTitle>
                </CardHeader>
                <CardContent>
                  <table className="w-full border-collapse text-left">
                    <thead>
                      <tr>
                        <th>Package</th>
                        <th>Dependents</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dependents.map((row) => (
                        <tr key={row.package_name}>
                          <td>{row.package_name}</td>
                          <td>{row.dependents}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </CardContent>
              </Card>
            </>
          )}
        </>
      )}
    </main>
  );
}
