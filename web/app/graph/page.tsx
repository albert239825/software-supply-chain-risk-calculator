"use client";
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function GraphExplorer() {
  const [packageId, setPackageId] = useState("");
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchGraph = async () => {
    setLoading(true);
    setError(null);
    setData(null);
    try {
      const res = await fetch(`/api/packages/${packageId}/graph`);
      if (!res.ok) throw new Error("Not found or error fetching graph");
      setData(await res.json());
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="mx-auto max-w-3xl px-6 py-16">
      <Card>
        <CardHeader>
          <CardTitle>Graph Explorer</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex gap-2">
            <input
              value={packageId}
              onChange={e => setPackageId(e.target.value)}
              placeholder="Enter package ID"
              className="border p-2 flex-1"
            />
            <button onClick={fetchGraph} className="p-2 bg-blue-500 text-white rounded">Explore</button>
          </div>
          {loading && <div>Loading...</div>}
          {error && <div className="text-red-500">{error}</div>}
          {data && <pre className="mt-4 whitespace-pre-wrap text-xs bg-muted p-2 rounded overflow-x-auto">{JSON.stringify(data, null, 2)}</pre>}
        </CardContent>
      </Card>
    </main>
  );
}
