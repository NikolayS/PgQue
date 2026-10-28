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
    await page.locator('#community').scrollIntoViewIfNeeded();
    await page.waitForLoadState('networkidle');
    const section = page.locator('#explainer');
    await section.scrollIntoViewIfNeeded();
    await expect(section.locator('#explainer-title')).toBeInViewport();
    await expect(page.locator('.hero .hero-visual')).toHaveCount(1);
    await expect(page.locator('.hero a[href="#explainer"]')).toHaveCount(0);
    expect(await page.evaluate(() => {
      const explainer = document.querySelector('#explainer')!;
      const clients = document.querySelector('#clients')!;
      const community = document.querySelector('#community')!;
      return !!(clients.compareDocumentPosition(explainer) & Node.DOCUMENT_POSITION_FOLLOWING)
        && !!(explainer.compareDocumentPosition(community) & Node.DOCUMENT_POSITION_FOLLOWING);
    })).toBe(true);
    expect(await page.locator('iframe').count()).toBe(0);
    expect(youtubeRequests).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
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

for (const videoId of ['bBNcR14Ts4U', 'ULpnamtBZFM']) {
  test(`community player retains keyboard focus: ${videoId}`, async ({ page }) => {
    await page.route('https://www.youtube-nocookie.com/**', route => route.fulfill({ body: '<html><body>Player</body></html>', contentType: 'text/html' }));
    await page.goto('/');
    const shell = page.locator(`[data-video-id="${videoId}"]`);
    await shell.locator('a').focus();
    await page.keyboard.press('Enter');
    await expect(shell.locator('iframe')).toHaveAttribute('src', `https://www.youtube-nocookie.com/embed/${videoId}?autoplay=1`);
    await expect(shell.locator('iframe')).toBeFocused();
  });
}
