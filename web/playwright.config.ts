import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests',
  use: { baseURL: 'http://127.0.0.1:4321', launchOptions: process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : {} },
  webServer: { command: 'bun run preview --host 127.0.0.1', url: 'http://127.0.0.1:4321' },
});
