import path from "node:path";
import { fileURLToPath } from "node:url";
import { config as loadDotenv } from "dotenv";
import { defineConfig } from "vitest/config";

const rootDir = path.dirname(fileURLToPath(import.meta.url));

loadDotenv({ path: path.join(rootDir, ".env.local") });

export default defineConfig({
  resolve: {
    alias: {
      "@": rootDir,
    },
  },
  test: {
    setupFiles: [path.join(rootDir, "vitest.setup.ts")],
    include: ["**/__tests__/**/*.test.{ts,tsx}"],
    environment: "jsdom",
    testTimeout: 20_000,
    hookTimeout: 20_000,
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
      include: [
        "app/**/*.{ts,tsx}",
        "components/**/*.{ts,tsx}",
        "lib/**/*.{ts,tsx}",
      ],
      exclude: [
        "**/*.d.ts",
        "**/__tests__/**",
        "**/*.config.*",
        "next-env.d.ts",
        // Heavy Next.js shells or routes tested only via higher-level smoke tests / manual QA.
        "app/layout.tsx",
        "app/api/github/**",
        "app/api/auth/callback/**",
        // Large client-only dashboards (still covered by UI smoke tests in __tests__).
        "app/graph/page.tsx",
        "app/track/page.tsx",
      ],
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 70,
        statements: 80,
      },
    },
  },
});
