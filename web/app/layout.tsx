import type { Metadata } from "next";
import Link from "next/link";
import { Geist, Geist_Mono } from "next/font/google";
import { AuthNav } from "@/components/auth/auth-nav";
import { SiteNav } from "@/components/site-nav";
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
        <nav className="sticky top-0 z-40 w-full border-b border-border bg-background/95 py-2.5 backdrop-blur">
          <div className="mx-auto flex w-full max-w-none items-center gap-3 px-6 text-sm">
            <Link
              href="/"
              className="mr-1 shrink-0 rounded-md px-2 py-1.5 font-semibold transition-colors hover:bg-muted/70"
            >
              Risk Scorer
            </Link>
            <SiteNav />
            <AuthNav />
          </div>
        </nav>
        {children}
      </body>
    </html>
  );
}
