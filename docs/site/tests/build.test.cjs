'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');
const { createHash } = require('node:crypto');
const { buildSite } = require('../build.cjs');
const { validate } = require('../evidence.js');

const document = (item) => ({ format: 'git-rs-architecture-evidence/v1', items: [item] });

test('evidence preserves measured zero, missing values and non-additive spans', () => {
  const [item] = validate(document({
    wall_seconds: 0, user_cpu_seconds: null, system_cpu_seconds: 'unknown',
    elapsed_spans: [{ name: 'Hash', seconds: 0.25, detail: 'Inclusive subset, not CPU' }],
  }));
  assert.equal(item.wall_seconds, 0);
  assert.equal(item.user_cpu_seconds, null);
  assert.equal(item.system_cpu_seconds, null);
  assert.deepEqual(item.elapsed_spans, [{ name: 'Hash', seconds: 0.25, detail: 'Inclusive subset, not CPU' }]);
  assert.deepEqual(validate(document({}))[0].elapsed_spans, []);
});

test('invalid elapsed spans and non-finite numbers cannot become measurements', () => {
  for (const value of [-1, Infinity, NaN, '0.25', null]) {
    assert.throws(() => validate(document({ elapsed_spans: [{ name: 'Hash', seconds: value, detail: '' }] })));
  }
  assert.throws(() => validate(document({ elapsed_spans: Array(21).fill({ name: '', seconds: 0, detail: '' }) })));
  assert.throws(() => validate(document({ wall_seconds: Infinity })));
  assert.throws(() => validate(document({ hotspots: [{ name: '', self_percent: 101, detail: '' }] })));
});

test('public build physically excludes local evidence, tests and source tooling', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'git-rs-atlas-public-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const output = path.join(root, 'site');
  buildSite({ output });
  assert.deepEqual(fs.readdirSync(output).sort(), [
    'index.html', 'style.css', 'architecture-data.js', 'guide-data.js', 'app.js', 'graph.js', 'graph.css',
    'evidence.js', 'favicon.svg', 'manifest.json', 'SHA256SUMS',
  ].sort());
  const manifest = JSON.parse(fs.readFileSync(path.join(output, 'manifest.json'), 'utf8'));
  assert.equal(manifest.localEvidence, false);
  assert.ok(!fs.readFileSync(path.join(output, 'index.html'), 'utf8').includes('src="local-evidence.js"'));
  for (const entry of manifest.files) {
    const bytes = fs.readFileSync(path.join(output, entry.name));
    assert.equal(createHash('sha256').update(bytes).digest('hex'), entry.sha256);
  }
  assert.throws(() => buildSite({ output }), /EEXIST/);
});

test('local evidence is explicit, inert text and absent from build metadata paths', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'git-rs-atlas-local-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const input = path.join(root, 'private-profile.json');
  const title = '</script><script>globalThis.executed=true</script>';
  fs.writeFileSync(input, JSON.stringify(document({ title, elapsed_spans: [{ name: 'Hash', seconds: 0, detail: '' }] })));
  const output = path.join(root, 'site');
  const manifest = buildSite({ output, evidencePath: input });
  assert.equal(manifest.localEvidence, true);
  const script = fs.readFileSync(path.join(output, 'local-evidence.js'), 'utf8');
  assert.ok(!script.includes('</script>'));
  const context = {};
  vm.runInNewContext(script, context);
  assert.equal(context.GIT_RS_LOCAL_EVIDENCE.items[0].title, title);
  assert.equal(context.executed, undefined);
  assert.ok(!JSON.stringify(manifest).includes(input));
  assert.ok(fs.readFileSync(path.join(output, 'index.html'), 'utf8').includes('src="local-evidence.js"'));
  const invalid = path.join(root, 'invalid.json');
  fs.writeFileSync(invalid, JSON.stringify(document({ elapsed_spans: [{ seconds: -1 }] })));
  const rejected = path.join(root, 'rejected');
  assert.throws(() => buildSite({ output: rejected, evidencePath: invalid }));
  assert.equal(fs.existsSync(rejected), false);
});

test('plain-language conclusions are bounded inert evidence and survive explicit builds', (t) => {
  assert.deepEqual(validate(document({ takeaways: ['Measured conclusion <script>'] }))[0].takeaways,
    ['Measured conclusion <script>']);
  for (const takeaways of ['not an array', Array(9).fill('x'), [42], ['x'.repeat(16001)]]) {
    assert.throws(() => validate(document({ takeaways })), /takeaways/);
  }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'git-rs-atlas-conclusions-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const input = path.join(root, 'profile.json');
  const output = path.join(root, 'site');
  const point = '</script><script>globalThis.executed=true</script>';
  fs.writeFileSync(input, JSON.stringify(document({ takeaways: [point] })));
  buildSite({ output, evidencePath: input });
  const context = {};
  vm.runInNewContext(fs.readFileSync(path.join(output, 'local-evidence.js'), 'utf8'), context);
  assert.equal(context.GIT_RS_LOCAL_EVIDENCE.items[0].takeaways[0], point);
  assert.equal(context.executed, undefined);
});

test('cache explanations map five structures to three actual graph components', () => {
  const context = {};
  for (const name of ['architecture-data.js', 'guide-data.js']) {
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', name), 'utf8'), context);
  }
  const groups = context.GIT_RS_GUIDE.cacheGroups;
  assert.deepEqual(JSON.parse(JSON.stringify(groups.map((group) => [group.component, ...group.layers]))), [
    ['source-cache', 'source-ram'], ['object-core', 'verified-decoded', 'header-preflight'],
    ['derived-cache', 'derived-body', 'derived-checkpoint'],
  ]);
  const components = new Set(context.GIT_RS_ARCH.components.map((entry) => entry.id));
  assert.ok(groups.every((group) => components.has(group.component)));
  assert.deepEqual(Object.keys(context.GIT_RS_GUIDE.findings).sort(),
    [...context.GIT_RS_ARCH.findings.map((finding) => finding.id)].sort());
});
