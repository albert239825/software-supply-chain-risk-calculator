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
      fetch("/api/stats/top-fanout").then(async (res) => {
        if (res.ok === false) throw new Error("Could not load fan-out stats");
        return res.json();
      }),
      fetch("/api/stats/most-dependents").then(async (res) => {
        if (res.ok === false) throw new Error("Could not load dependents stats");
        return res.json();
      }),
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
    <main className="mx-auto max-w-5xl space-y-6 px-6 py-12">
      <header>
        <h1 className="text-2xl font-semibold">Ecosystem statistics</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm">
          Quick rankings for dependency fan-out and ecosystem blast radius.
        </p>
      </header>

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
                  <CardTitle>Highest direct fan-out</CardTitle>
                  <CardDescription>
                    Packages with the largest number of direct dependencies.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Package</th>
                          <th className="text-right">Direct dependencies</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fanout.map((row) => (
                          <tr key={row.package_name}>
                            <td className="font-medium">{row.package_name}</td>
                            <td className="text-right tabular-nums">
                              {Number(row.num_dependencies).toLocaleString()}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Most depended-on packages</CardTitle>
                  <CardDescription>
                    Packages with the most inbound dependency edges.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Package</th>
                          <th className="text-right">Dependents</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dependents.map((row) => (
                          <tr key={row.package_name}>
                            <td className="font-medium">{row.package_name}</td>
                            <td className="text-right tabular-nums">
                              {Number(row.dependents).toLocaleString()}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </>
      )}
    </main>
  );
}
