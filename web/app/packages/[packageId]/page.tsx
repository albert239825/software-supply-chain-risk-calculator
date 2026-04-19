"use client";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function PackageDetailPage() {
  const { packageId } = useParams<{ packageId: string }>();
  const [data, setData] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!packageId) return;
    fetch(`/api/packages/${packageId}/versions`)
      .then((res) => res.json())
      .then((rows) => {
        setData(rows);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, [packageId]);

  return (
    <main className="mx-auto max-w-3xl px-6 py-16">
      <Card>
        <CardHeader>
          <CardTitle>Package Detail</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <div>Loading...</div>}
          {error && <div className="text-red-500">{error}</div>}
          {!loading && !error && data ? (
            <pre className="whitespace-pre-wrap text-xs bg-muted p-2 rounded overflow-x-auto">{JSON.stringify(data, null, 2)}</pre>
          ) : null}
        </CardContent>
      </Card>
    </main>
  );
}
