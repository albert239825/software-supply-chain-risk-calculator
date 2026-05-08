import type { Metadata } from "next";
import Link from "next/link";
import { Geist, Geist_Mono } from "next/font/google";
import { ShieldCheck } from "lucide-react";
import { AuthNav } from "@/components/auth/auth-nav";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Software Supply Chain Risk Scorer",
  description:
    "Explore the NPM package ecosystem via an interactive dependency graph and a composite risk score.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col bg-background text-foreground">
        <nav className="sticky top-0 z-20 w-full border-b border-border bg-background/90 py-3 backdrop-blur">
          <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-3 px-6">
            <Link href="/" className="mr-2 flex items-center gap-2 font-semibold">
              <span className="flex size-8 items-center justify-center rounded-md bg-primary text-primary-foreground">
                <ShieldCheck className="size-4" />
              </span>
              Risk Scorer
            </Link>
            <Link href="/risk" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Risk</Link>
            <Link href="/stats" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Stats</Link>
            <Link href="/maintainers" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Maintainers</Link>
            <Link href="/packages" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Packages</Link>
            <Link href="/abandoned" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Abandoned</Link>
            <Link href="/no-repo" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">No Repo</Link>
            <Link href="/graph" className="rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground">Graph</Link>
            <Link href="/track" className="rounded-md bg-secondary px-2.5 py-1.5 text-sm font-semibold text-secondary-foreground transition-colors hover:bg-secondary/80">Tracked</Link>
            <AuthNav />
          </div>
        </nav>
        {children}
      </body>
    </html>
  );
}
