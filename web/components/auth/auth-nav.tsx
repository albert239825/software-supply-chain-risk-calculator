"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { LogOut } from "lucide-react";
import { Button } from "@/components/ui/button";

type User = {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
};

export function AuthNav() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/auth/me")
      .then((res) => res.json())
      .then((body: { user: User | null }) => {
        setUser(body.user);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    setUser(null);
    window.location.href = "/";
  }

  if (loading) {
    return null;
  }

  if (!user) {
    return (
      <div className="ml-auto flex items-center gap-2">
        <Button asChild variant="outline" size="sm">
          <Link href="/track">Set up login</Link>
        </Button>
      </div>
    );
  }

  return (
    <div className="ml-auto flex min-w-0 items-center gap-2">
      <Button asChild variant="ghost" size="sm">
        <Link href="/track" className="max-w-40 truncate">
          {user.displayName || user.email || "Account"}
        </Link>
      </Button>
      <Button
        aria-label="Log out"
        title="Log out"
        variant="ghost"
        size="icon-sm"
        onClick={logout}
      >
        <LogOut />
      </Button>
    </div>
  );
}
