import path from "node:path";
import { fileURLToPath } from "node:url";
import { config as loadDotenv } from "dotenv";
import { defineConfig } from "vitest/config";

const rootDir = path.dirname(fileURLToPath(import.meta.url));

// Load SUPABASE_DB_URL (and anything else) from .env.local so `npm test`
// behaves the same as `npm run dev`. Next.js auto-loads .env.local; vitest
// does not.
loadDotenv({ path: path.join(rootDir, ".env.local") });

export default defineConfig({
  resolve: {
    alias: {
      "@": rootDir,
    },
  },
  test: {
    include: ["**/__tests__/**/*.test.ts"],
    environment: "node",
    // Integration tests hit real Supabase and are slower than the 300ms
    // unit-test baseline.
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
});
