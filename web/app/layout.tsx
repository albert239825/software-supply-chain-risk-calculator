import type { Metadata } from "next";
import Link from "next/link";
import { Geist, Geist_Mono } from "next/font/google";
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
        <nav className="w-full border-b border-border bg-muted/50 py-3 mb-8">
          <div className="mx-auto flex max-w-3xl gap-6 px-6">
            <Link href="/" className="font-medium hover:underline">Home</Link>
            <Link href="/risk" className="font-medium hover:underline">Risk Analysis</Link>
            <Link href="/stats" className="font-medium hover:underline">Stats</Link>
            <Link href="/maintainers" className="font-medium hover:underline">Maintainers</Link>
            <Link href="/packages" className="font-medium hover:underline">Packages</Link>
            <Link href="/abandoned" className="font-medium hover:underline">Abandoned</Link>
            <Link href="/no-repo" className="font-medium hover:underline">No Repo</Link>
            <Link href="/graph" className="font-medium hover:underline">Graph Explorer</Link>
          </div>
        </nav>
        {children}
      </body>
    </html>
  );
}
