import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests',
  forbidOnly: !!process.env.CI,
  // Do not hide failed assertions behind automatic retries.
  retries: 0,
  use: { baseURL: 'http://127.0.0.1:4321', launchOptions: process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : {} },
  // Fail on an occupied port rather than test an unrelated or stale server.
  webServer: { command: 'bun run build && bun run preview --host 127.0.0.1', url: 'http://127.0.0.1:4321', timeout: 120_000 },
});
