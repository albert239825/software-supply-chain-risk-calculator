"use client";
import { useEffect, useState } from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface RiskRow {
  package_name: string;
  maintainers: number;
  dependencies: number;
  last_release: string;
  risk_score: number;
}

export default function RiskAnalysisPage() {
  const [data, setData] = useState<RiskRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/risk/ranked")
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
          <CardTitle>Top Risky Packages</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <div>Loading...</div>}
          {error && <div className="text-red-500">{error}</div>}
          {!loading && !error && (
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  <th>Package</th>
                  <th>Maintainers</th>
                  <th>Dependencies</th>
                  <th>Last Release</th>
                  <th>Risk Score</th>
                </tr>
              </thead>
              <tbody>
                {/* Filter and sort data: valid risk_score first, i put N/A at the bottom but we can also remove*/}
                {data.filter(
                  (row) =>
                    row.risk_score !== null &&
                    row.risk_score !== undefined &&
                    !isNaN(Number(row.risk_score))
                ).sort((a, b) => Number(b.risk_score) - Number(a.risk_score)).map(
                  (row) => (
                    <tr key={row.package_name}>
                      <td>{row.package_name}</td>
                      <td>{row.maintainers}</td>
                      <td>{row.dependencies}</td>
                      <td>{row.last_release?.slice(0, 10)}</td>
                      <td>
                        {row.risk_score !== null && row.risk_score !== undefined && !isNaN(Number(row.risk_score))
                          ? Number(row.risk_score).toFixed(2)
                          : "N/A"}
                      </td>
                    </tr>
                  )
                )}
                {data.filter(
                  (row) =>
                    row.risk_score === null ||
                    row.risk_score === undefined ||
                    isNaN(Number(row.risk_score))
                ).map((row) => (
                  <tr key={row.package_name}>
                    <td>{row.package_name}</td>
                    <td>{row.maintainers}</td>
                    <td>{row.dependencies}</td>
                    <td>{row.last_release?.slice(0, 10)}</td>
                    <td>N/A</td>
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
