import { test, expect } from '@playwright/test';

for (const width of [390, 1440]) {
  test(`explainer placement and keyboard playback at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1000 });
    const youtubeRequests: string[] = [];
    page.on('request', request => {
      if (/youtube|ytimg/.test(request.url())) youtubeRequests.push(request.url());
    });
    // Deterministic embed boundary: do not depend on YouTube availability in CI.
    await page.route('https://www.youtube-nocookie.com/**', route => route.fulfill({ body: '<html><body>Player</body></html>', contentType: 'text/html' }));
    await page.goto('/');
    await page.getByRole('link', { name: 'Watch the explainer', exact: true }).click();
    const section = page.locator('#explainer');
    await expect(section).toBeVisible();
    expect(await page.locator('iframe').count()).toBe(0);
    expect(youtubeRequests).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    expect(await page.evaluate(() => !!(document.querySelector('#explainer')!.compareDocumentPosition(document.querySelector('.hero-visual')!) & Node.DOCUMENT_POSITION_FOLLOWING))).toBe(true);
    const play = section.getByRole('link', { name: 'Play the PgQue explainer' });
    await play.focus();
    await page.keyboard.press('Enter');
    const frame = section.locator('iframe');
    await expect(frame).toHaveAttribute('src', 'https://www.youtube-nocookie.com/embed/1My8l2fQ2NA?autoplay=1');
    await expect(frame).toHaveAttribute('title', 'Why most Postgres queues fail (and how PgQue fixes it)');
    await expect(frame).toBeFocused();
    await expect(section.getByRole('link', { name: 'Watch on YouTube' })).toHaveAttribute('href', 'https://www.youtube.com/watch?v=1My8l2fQ2NA');
  });
}

test('no-JavaScript fallback remains a normal video link', async ({ browser, baseURL }) => {
  const context = await browser.newContext({ javaScriptEnabled: false, baseURL });
  const page = await context.newPage();
  await page.goto('/');
  await expect(page.getByRole('link', { name: 'Play the PgQue explainer' })).toHaveAttribute('href', 'https://www.youtube.com/watch?v=1My8l2fQ2NA');
  await expect(page.locator('iframe')).toHaveCount(0);
  await context.close();
});
