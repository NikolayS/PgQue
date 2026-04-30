import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: false,
    include: ['test/**/*.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Tests share a single Postgres database via PGQUE_TEST_DSN. Running
    // suites in parallel races on queue/consumer lifecycle (drop_queue in
    // one suite's afterEach overlapping with another suite's beforeEach
    // create_queue), so serialise across files.
    fileParallelism: false,
    typecheck: {
      tsconfig: './tsconfig.test.json',
    },
  },
});
