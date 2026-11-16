'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require('playwright-core');

const site = path.resolve(__dirname, '..');
const files = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/index.html', ['index.html', 'text/html; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/architecture-data.js', ['architecture-data.js', 'text/javascript; charset=utf-8']],
  ['/style.css', ['style.css', 'text/css; charset=utf-8']],
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
  }],
};

async function exercise(page, address) {
  const errors = [];
  const externalRequests = [];
  const base = new URL('.', address);
  const allowed = new Set([address, ...['app.js', 'architecture-data.js', 'style.css'].map((file) => new URL(file, base).href)]);
  page.setDefaultTimeout(10000);
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('request', (request) => {
    if (!allowed.has(request.url())) externalRequests.push(request.url());
  });
  await page.goto(address, { waitUntil: 'load' });
  await page.waitForSelector('#component-map button');
  assert.ok(await page.locator('#component-map button').count() >= 16);
  assert.ok(await page.locator('#flow-select option').count() >= 8);
  assert.equal(await page.locator('#tab-overview').getAttribute('aria-selected'), 'true');
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
  assert.ok(await page.locator('.finding-card').filter({ hasText: 'P1' }).locator('.badge-critical').count() >= 1);
  await page.locator('#tab-reviews').click();
  assert.ok(await page.locator('.review-card').count() >= 4);
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
  assert.ok((await content.innerText()).includes('нет измерения'));
  assert.ok(!(await content.innerText()).includes('измеренный профиль — mapped core'));
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1), false, 'resource table must scroll inside page');
  await page.locator('#tab-flows').click();
  const flows = await page.evaluate(() => globalThis.GIT_RS_ARCH.flows.map(({ id, steps }) => ({ id, titles: steps.map(({ title }) => title) })));
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
  if (process.env.EVIDENCE_PATH) {
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
  assert.deepEqual(errors, []);
  assert.deepEqual(externalRequests, []);
}

(async () => {
  const server = http.createServer((request, response) => {
    const entry = files.get(new URL(request.url, 'http://127.0.0.1').pathname);
    if (!entry) { response.writeHead(404).end(); return; }
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
    await exercise(desktop, `http://127.0.0.1:${server.address().port}/`);
    await desktop.close();
    const mobile = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true });
    await exercise(mobile, `file://${site}/index.html`);
    await mobile.close();
    console.log('PASS: desktop HTTP and mobile file navigation, evidence and offline checks');
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
