import type { Metadata } from "next";
import Link from "next/link";
import { Geist, Geist_Mono } from "next/font/google";
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

const navItems = [
  { href: "/risk", label: "Risk" },
  { href: "/graph", label: "Graph" },
  { href: "/stats", label: "Stats" },
  { href: "/packages", label: "Packages" },
  { href: "/track", label: "Tracked" },
  { href: "/maintainers", label: "Maintainers" },
  { href: "/abandoned", label: "Abandoned" },
  { href: "/no-repo", label: "No Repo" },
];

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
        <nav className="w-full border-b border-border bg-background/95 py-3">
          <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-x-5 gap-y-2 px-6 text-sm">
            <Link href="/" className="mr-2 font-semibold hover:underline">
              Risk Scorer
            </Link>
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="text-muted-foreground hover:text-foreground"
              >
                {item.label}
              </Link>
            ))}
            <AuthNav />
          </div>
        </nav>
        {children}
      </body>
    </html>
  );
}
