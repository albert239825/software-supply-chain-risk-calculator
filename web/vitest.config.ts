import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const rootDir = path.dirname(fileURLToPath(import.meta.url));

/**
 * Vitest config for the Phase 0 unit tests. We only mirror the `@/*` path
 * alias from `tsconfig.json` — no DOM environment, no Next.js plugin. The
 * modules under test (`lib/risk/score.ts`, `lib/api/params.ts`) are pure
 * TypeScript and run natively under Vitest's esbuild transform.
 */
export default defineConfig({
  resolve: {
    alias: {
      "@": rootDir,
    },
  },
  test: {
    include: ["**/__tests__/**/*.test.ts"],
    environment: "node",
  },
});
