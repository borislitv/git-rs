'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require('playwright-core');

const site = path.resolve(process.env.SITE_DIRECTORY || path.join(__dirname, '..'));
const files = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/index.html', ['index.html', 'text/html; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/architecture-data.js', ['architecture-data.js', 'text/javascript; charset=utf-8']],
  ['/guide-data.js', ['guide-data.js', 'text/javascript; charset=utf-8']],
  ['/style.css', ['style.css', 'text/css; charset=utf-8']],
  ['/graph.js', ['graph.js', 'text/javascript; charset=utf-8']],
  ['/graph.css', ['graph.css', 'text/css; charset=utf-8']],
  ['/evidence.js', ['evidence.js', 'text/javascript; charset=utf-8']],
  ['/favicon.svg', ['favicon.svg', 'image/svg+xml']],
  ['/local-evidence.js', ['local-evidence.js', 'text/javascript; charset=utf-8']],
  ['/graphs/git-rs.html', ['graphs/git-rs.html', 'text/html; charset=utf-8']],
]);
const evidence = {
  format: 'git-rs-architecture-evidence/v1',
  items: [{
    id: 'synthetic', title: '<img src=x onerror=alert(1)>',
    scope: 'range-backed synthetic test', revision: 'older-revision', workload: 'fixture',
    flow_id: 'blame-cold', step_id: 'bc-core',
    wall_seconds: 0, user_cpu_seconds: null, peak_rss_bytes: 1024,
    outcome: 'partial', limitations: 'synthetic only', source: 'not a link',
    hotspots: [{ name: '<script>bad()</script>', self_percent: 23.5, detail: 'synthetic sample' }],
    elapsed_spans: [{ name: 'Hash fixture', seconds: 0.125, detail: 'synthetic inclusive elapsed' }],
    takeaways: ['Synthetic conclusion <img src=x onerror=alert(1)>'],
  }],
};

async function exercise(page, address) {
  const errors = [];
  const externalRequests = [];
  const base = new URL('.', address);
  const allowed = new Set([address, ...['app.js', 'architecture-data.js', 'guide-data.js', 'style.css', 'graph.js', 'graph.css', 'evidence.js', 'local-evidence.js', 'favicon.svg'].map((file) => new URL(file, base).href)]);
  page.setDefaultTimeout(10000);
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
  page.on('request', (request) => {
    if (!allowed.has(request.url())) externalRequests.push(request.url());
  });
  await page.goto(address, { waitUntil: 'load' });
  await page.waitForSelector('.grg-node');
  const bundledItems = await page.evaluate(() => globalThis.GIT_RS_LOCAL_EVIDENCE?.items.length || 0);
  if (bundledItems) {
    assert.equal(await page.locator('#evidence-content .imported-evidence').count(), bundledItems);
    const conclusions = await page.evaluate(() => globalThis.GIT_RS_LOCAL_EVIDENCE.items.filter((item) => item.takeaways?.length).length);
    assert.equal(await page.locator('.performance-observation').count(), conclusions);
    assert.equal(await page.locator('#local-profile-notice').isVisible(), true);
    await page.locator('#local-profile-notice a').click();
    assert.equal(await page.locator('#panel-resources').evaluate((panel) => panel.contains(document.activeElement)), true);
    if (process.env.SCREENSHOT_DIRECTORY) {
      await page.locator('#performance-observations').evaluate((node) => node.scrollIntoView({ block: 'start' }));
      await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIRECTORY, `measurements-${page.viewportSize().width}.png`) });
    }
    await page.locator('#tab-overview').click();
  }
  assert.ok(await page.locator('#component-map button').count() >= 16);
  assert.ok(await page.locator('#flow-select option').count() >= 8);
  if (await page.locator('#archify-link').count()) {
    const [companion] = await Promise.all([
      page.waitForEvent('popup'), page.locator('#archify-link').click(),
    ]);
    await companion.waitForLoadState('load');
    assert.ok(companion.url().endsWith('/graphs/git-rs.html'));
    assert.equal(await companion.locator('html').getAttribute('lang'), 'ru');
    assert.ok(await companion.locator('svg').count() > 0);
    assert.ok((await companion.locator('body').innerText()).includes('Source PostgreSQL'));
    if (process.env.SCREENSHOT_DIRECTORY) await companion.screenshot({
      path: path.join(process.env.SCREENSHOT_DIRECTORY, `archify-${page.viewportSize().width}.png`),
    });
    await companion.close();
  }
  assert.equal(await page.locator('#tab-overview').getAttribute('aria-selected'), 'true');
  assert.equal(await page.locator('.grg-node').count(), 16);
  assert.equal(await page.locator('.grg-edge').evaluateAll((edges) => edges.some((edge) => /NaN|Infinity/.test(edge.getAttribute('d')))), false, 'all SVG transitions must have finite geometry');
  assert.equal(await page.locator('.grg-flow option').count(), 9);
  await page.getByRole('button', { name: 'Увеличить карту', exact: true }).click();
  assert.equal(await page.locator('.grg-zoom').innerText(), '125%');
  await page.getByRole('button', { name: 'Сброс', exact: true }).click();
  assert.equal(await page.locator('.grg-zoom').innerText(), '100%');
  await page.getByRole('button', { name: 'Вместить', exact: true }).click();
  assert.ok(Number.parseInt(await page.locator('.grg-zoom').innerText(), 10) < 100);
  await page.getByRole('button', { name: 'Сброс', exact: true }).click();
  const viewport = page.locator('.grg-viewport');
  await viewport.scrollIntoViewIfNeeded();
  await viewport.evaluate((node) => { node.scrollTop = 200; });
  const bounds = await viewport.boundingBox();
  await page.mouse.move(bounds.x + 8, bounds.y + 80);
  await page.mouse.down();
  await page.mouse.move(bounds.x + 8, bounds.y + 40, { steps: 4 });
  await page.mouse.up();
  assert.ok(await viewport.evaluate((node) => node.scrollTop) > 220, 'pointer pan must move the graph');
  await page.locator('.grg-search').fill('metadata');
  await page.locator('.grg-search').press('Enter');
  await page.keyboard.press('Enter');
  assert.ok((await page.locator('#component-detail').innerText()).includes('PostgreSQL'));
  assert.equal(await page.locator('.grg-node[data-component="metadata"]').getAttribute('aria-pressed'), 'true');
  await page.locator('.grg-search').fill('');
  const graphFlows = await page.evaluate(() => globalThis.GIT_RS_ARCH.flows.map((flow) => flow.title));
  for (const [index, title] of graphFlows.entries()) {
    await page.locator('.grg-flow').selectOption(String(index));
    assert.ok((await page.locator('.grg-route h4').innerText()).includes(title));
    assert.ok(await page.locator('.grg-participating').count() > 0);
  }
  await page.getByRole('button', { name: 'Разобрать шаги', exact: true }).click();
  assert.equal(await page.locator('#flow-select').inputValue(), '7');
  assert.equal(await page.locator('#panel-flows').evaluate((panel) => panel.contains(document.activeElement)), true, 'focus must leave hidden graph panel');
  await page.locator('#tab-overview').click();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false, 'graph must scroll inside page');
  await page.locator('#tab-flows').click();
  await page.goBack();
  await page.waitForFunction(() => document.querySelector('#tab-overview').getAttribute('aria-selected') === 'true');
  await page.locator('#tab-overview').focus();
  await page.keyboard.press('End');
  assert.equal(await page.locator('#tab-reviews').getAttribute('aria-selected'), 'true');
  await page.keyboard.press('Home');
  assert.equal(await page.locator('#tab-overview').getAttribute('aria-selected'), 'true');
  await page.locator('#tab-flows').click();
  await page.evaluate(() => { window.location.hash = '#unknown-route'; });
  await page.waitForFunction(() => document.querySelector('#tab-overview').getAttribute('aria-selected') === 'true');
  await page.locator('#tab-findings').click();
  assert.ok(await page.locator('.finding-card').count() >= 12);
  assert.equal(await page.locator('.finding-card').first().locator('.finding-kind').innerText(), 'Исправленный дефект');
  assert.equal(await page.locator('.finding-card').first().locator('.badge-critical').count(), 0, 'fixed catalog finding is no longer the old P1');
  const collision = page.locator('[data-finding="sha1-collision-check-required"]');
  assert.ok((await collision.innerText()).includes('не обнаруженный дефект'));
  assert.ok((await collision.innerText()).includes('Подменить защищённый SHA-1'));
  assert.equal(await page.locator('.finding-card .technical-details[open]').count(), 0);
  await page.locator('#tab-readiness').click();
  assert.ok((await page.locator('#readiness-verdict').innerText()).includes('Большой blame ещё не доведён'));
  assert.equal(await page.locator('.readiness-card').count(), 6);
  assert.equal(await page.locator('.safeguard-card').count(), 3);
  if (process.env.SCREENSHOT_DIRECTORY) {
    await page.locator('#panel-readiness').evaluate((node) => node.scrollIntoView({ block: 'start' }));
    await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIRECTORY, `readiness-${page.viewportSize().width}.png`) });
  }
  await page.locator('#tab-decisions').click();
  assert.equal(await page.locator('.decision-card').count(), 6);
  assert.ok((await page.locator('[data-decision="warm"]').innerText()).includes('30 секунд'));
  assert.ok((await page.locator('[data-decision="cpu"]').innerText()).includes('не распараллелят'));
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false);
  if (process.env.SCREENSHOT_DIRECTORY) {
    await page.locator('#panel-decisions').evaluate((node) => node.scrollIntoView({ block: 'start' }));
    await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIRECTORY, `decisions-${page.viewportSize().width}.png`) });
  }
  await page.locator('#tab-reviews').click();
  assert.ok(await page.locator('.review-card').count() >= 4);
  await page.locator('#tab-caches').click();
  assert.equal(await page.locator('.cache-group').count(), 3);
  assert.equal(await page.locator('.cache-layer').count(), 5);
  assert.deepEqual(await page.locator('.cache-group').evaluateAll((groups) => groups.map((group) => group.querySelectorAll('.cache-layer').length)), [1, 2, 2]);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false);
  if (process.env.SCREENSHOT_DIRECTORY) {
    await page.locator('#panel-caches').evaluate((node) => node.scrollIntoView({ block: 'start' }));
    await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIRECTORY, `caches-${page.viewportSize().width}.png`) });
  }
  for (const component of ['source-cache', 'object-core', 'derived-cache']) {
    await page.locator(`#cache-topology .cache-map-button[data-component="${component}"]`).click();
    assert.equal(await page.locator('#tab-overview').getAttribute('aria-selected'), 'true');
    assert.equal(await page.locator('.grg-flow').inputValue(), '', 'cache location navigation must clear an unrelated request path');
    assert.equal(await page.locator(`.grg-node[data-component="${component}"]`).getAttribute('aria-pressed'), 'true');
    assert.equal(await page.locator(`.grg-node[data-component="${component}"]`).evaluate((node) => node === document.activeElement), true);
    await page.locator('#tab-caches').click();
  }
  await page.locator('#tab-parameters').click();
  assert.equal(await page.locator('.parameter-card').count(), 15);
  await page.locator('#tab-resources').click();
  await page.locator('#evidence-file').setInputFiles({
    name: 'synthetic.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify(evidence)),
  });
  await page.waitForFunction(() => document.querySelector('#evidence-status').textContent.includes('загружено'));
  const content = page.locator('#evidence-content');
  assert.equal(await content.locator('img').count(), 0);
  assert.equal(await content.locator('script').count(), 0);
  assert.ok((await content.innerText()).includes('<script>bad()</script>'));
  assert.ok((await content.innerText()).includes('23,5'));
  assert.ok((await content.innerText()).includes('synthetic inclusive elapsed'));
  assert.ok((await page.locator('#performance-observations').innerText()).includes('Synthetic conclusion <img'));
  assert.equal(await page.locator('#performance-observations img').count(), 0);
  assert.equal(await content.locator('meter').count(), 3);
  assert.ok((await content.innerText()).includes('нет измерения'));
  assert.ok(!(await content.innerText()).includes('измеренный профиль — mapped core'));
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false, 'resource table must scroll inside page');
  await page.locator('#tab-flows').click();
  const flows = await page.evaluate(() => globalThis.GIT_RS_ARCH.flows.map(({ id, steps }) => ({ id, titles: steps.map(({ title }) => title) })));
  assert.equal(flows.reduce((count, flow) => count + flow.titles.length, 0), 35);
  for (const [flowIndex, flow] of flows.entries()) {
    await page.locator('#flow-select').selectOption(String(flowIndex));
    assert.equal(await page.locator('#step-diagram button').count(), flow.titles.length);
    for (const [index, title] of flow.titles.entries()) {
      await page.locator('#step-diagram button').nth(index).click();
      assert.equal(await page.locator('#step-detail h3').innerText(), title);
    }
  }
  const flowIndex = flows.findIndex((flow) => flow.id === 'blame-cold');
  await page.locator('#flow-select').selectOption(String(flowIndex));
  const stepIndex = await page.evaluate(() => globalThis.GIT_RS_ARCH.flows.find((flow) => flow.id === 'blame-cold').steps.findIndex((step) => step.id === 'bc-core'));
  await page.locator('#step-diagram button').nth(stepIndex).click();
  assert.ok((await page.locator('#step-detail').innerText()).includes('synthetic sample'));
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false, 'page must not overflow its viewport');
  await page.locator('#tab-resources').click();
  await page.locator('#evidence-file').setInputFiles({ name: 'invalid.json', mimeType: 'application/json', buffer: Buffer.from('{') });
  await page.waitForFunction(() => document.querySelector('#evidence-status').textContent.includes('отклонён'));
  assert.ok((await content.innerText()).includes('synthetic sample'), 'invalid input keeps last good evidence');
  await page.locator('#clear-evidence').click();
  assert.equal(await content.locator('meter').count(), 0);
  assert.equal(await page.locator('.performance-observation').count(), 0, 'clearing evidence also clears its conclusions');
  const storageCount = await page.evaluate(() => {
    try { return localStorage.length + sessionStorage.length; }
    catch (error) {
      if (window.location.protocol === 'file:' && error.name === 'SecurityError') return null;
      throw error;
    }
  });
  if (storageCount !== null) assert.equal(storageCount, 0);
  await page.locator('#tab-flows').click();
  assert.ok(!(await page.locator('#step-detail').innerText()).includes('synthetic sample'));
  await page.locator('#tab-resources').click();
  await page.locator('#evidence-file').setInputFiles({
    name: 'context-only.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify({
      format: evidence.format, items: [{ flow_id: 'http-fetch', step_id: 'hf-entry', title: 'Context without numbers' }],
    })),
  });
  await page.waitForFunction(() => document.querySelector('#evidence-status').textContent.includes('загружено'));
  assert.ok((await page.locator('#measurement-coverage').innerText()).includes('Контекстных evidence без чисел: 1'));
  assert.ok((await page.locator('#deployment-scope').innerText()).includes('локального файла'));
  await page.locator('#clear-evidence').click();
  if (process.env.EVIDENCE_PATH && !process.env.BASE_URL?.startsWith('https:')) {
    await page.locator('#tab-resources').click();
    await page.locator('#evidence-file').setInputFiles(process.env.EVIDENCE_PATH);
    await page.waitForFunction(() => document.querySelector('#evidence-status').textContent.includes('загружено'));
    assert.ok(await page.locator('.imported-evidence').count() > 0, 'evidence file has visible entries');
    const items = JSON.parse(fs.readFileSync(process.env.EVIDENCE_PATH, 'utf8')).items;
    const binding = await page.evaluate((entries) => {
      for (const item of entries) {
        const flowIndex = globalThis.GIT_RS_ARCH.flows.findIndex((flow) => flow.id === item.flow_id);
        if (flowIndex < 0) continue;
        const stepIndex = globalThis.GIT_RS_ARCH.flows[flowIndex].steps.findIndex((step) => step.id === item.step_id);
        if (stepIndex >= 0) return { flowIndex, stepIndex };
      }
      return null;
    }, items);
    if (binding) {
      await page.locator('#tab-flows').click();
      await page.locator('#flow-select').selectOption(String(binding.flowIndex));
      await page.locator('#step-diagram button').nth(binding.stepIndex).click();
      assert.ok(await page.locator('#step-detail .imported-evidence').count() > 0, 'bound evidence appears on its step');
      await page.locator('#tab-resources').click();
    }
    await page.locator('#clear-evidence').click();
  }
  if (process.env.SCREENSHOT_DIRECTORY) {
    await page.locator('#tab-overview').click();
    await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIRECTORY, `architecture-${page.viewportSize().width}.png`), fullPage: true });
  }
  assert.deepEqual(errors, []);
  assert.deepEqual(externalRequests, []);
}

(async () => {
  const server = http.createServer((request, response) => {
    const entry = files.get(new URL(request.url, 'http://127.0.0.1').pathname);
    if (!entry || !fs.existsSync(path.join(site, entry[0]))) { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'Content-Type': entry[1] });
    fs.createReadStream(path.join(site, entry[0])).pipe(response);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const options = { headless: true, args: ['--disable-background-networking', '--disable-dev-shm-usage'] };
  if (process.env.CHROMIUM_PATH) options.executablePath = process.env.CHROMIUM_PATH;
  if (process.getuid?.() === 0) options.args.push('--no-sandbox');
  let browser;
  try {
    browser = await chromium.launch(options);
    const desktop = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await exercise(desktop, process.env.BASE_URL || `http://127.0.0.1:${server.address().port}/`);
    await desktop.close();
    const mobile = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true });
    await exercise(mobile, process.env.BASE_URL || `file://${site}/index.html`);
    await mobile.close();
    console.log('PASS: desktop/mobile graph, 8 flows, 35 steps, cache layers, parameters, evidence and offline request checks');
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
