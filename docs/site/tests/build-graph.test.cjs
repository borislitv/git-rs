'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { verifyReceipt } = require('../build-graph.cjs');

test('graph receipt accepts normalized repository identity and rejects failed evidence', () => {
  const stages = Object.fromEntries(['validate', 'deliver', 'check', 'browser-check']
    .map((name) => [name, { status: 'pass', exitCode: 0 }]));
  stages.deliver.receipt = {
    update: { reason: 'disabled' },
    evidence: { verified: true, repository: 'https://github.com/borislitv/git-rs', revision: 'da6f02f1cba3852c899fc74e129fba20556e5cf8' },
  };
  stages.check.receipt = { provenance: 'current', composition: { status: 'pass' } };
  const common = {
    ok: true, status: 'pass', command: 'finalize', type: 'architecture', quality: 'showcase',
    specification: { sha256: 'candidate' }, artifact: { sha256: 'html' },
    browserCheck: { requested: true, ran: true },
  };
  const summary = { ...common, gates: { validate: 'pass', deliver: 'pass', check: 'pass', 'browser-check': 'pass' } };
  const receipt = { ...common, stages, update: { reason: 'disabled' } };
  verifyReceipt(summary, receipt, 'candidate', 'html');
  stages.deliver.receipt.evidence.verified = false;
  assert.throws(() => verifyReceipt(summary, receipt, 'candidate', 'html'), /verify pinned source evidence/);
  stages.deliver.receipt.evidence.verified = true;
  stages['browser-check'].status = 'fail';
  assert.throws(() => verifyReceipt(summary, receipt, 'candidate', 'html'), /browser-check/);
});
