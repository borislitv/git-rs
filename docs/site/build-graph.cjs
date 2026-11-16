'use strict';

// Offline single-graph adapter; never installs tools or changes source repositories.
// node docs/site/build-graph.cjs --tool-root PATH --repo-root PATH --out NEWDIR
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { spawnSync } = require('node:child_process');
const { pathToFileURL } = require('node:url');

const here = fs.realpathSync(__dirname);
const pinFile = path.join(here, 'archify-pin.json');
const pin = readJson(pinFile);
const env = {
  ...process.env,
  ARCHIFY_UPDATE_CHECK_DISABLED: '1',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_NO_LAZY_FETCH: '1',
  GIT_TERMINAL_PROMPT: '0',
  GIT_NO_REPLACE_OBJECTS: '1',
  GIT_ALLOW_PROTOCOL: '',
};

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function writeJson(file, value) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
}

function inside(root, target) {
  const relative = path.relative(root, target);
  return relative === '' || (!path.isAbsolute(relative) && relative !== '..' &&
    !relative.startsWith(`..${path.sep}`));
}

// Same package-tree algorithm as the pinned GeeseFS builder. Reject links/devices.
function packageTreeDigest(root) {
  const hash = createHash('sha256');
  function visit(relative = '') {
    const entries = fs.readdirSync(path.join(root, relative), { withFileTypes: true });
    entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
    for (const entry of entries) {
      const name = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) visit(name);
      else if (entry.isFile()) {
        hash.update(`${name}\0${sha256(fs.readFileSync(path.join(root, name)))}\n`);
      } else throw new Error(`Non-regular tool package entry: ${name}`);
    }
  }
  visit();
  return hash.digest('hex');
}

function spawn(program, args, cwd) {
  return spawnSync(program, args, {
    cwd, env, timeout: 150_000, maxBuffer: 16 * 1024 * 1024,
    killSignal: 'SIGKILL', windowsHide: true,
  });
}

function git(root, args) {
  const result = spawn('git', [
    '--no-replace-objects', '-c', 'protocol.allow=never', '-C', root, ...args,
  ], root);
  if (result.error || result.status !== 0) {
    throw new Error(`Read-only Git ${args[0]} failed: ${result.error?.message ||
      result.stderr?.toString('utf8') || result.signal || result.status}`);
  }
  return result.stdout;
}

function verifySource(root, source) {
  if (!fs.existsSync(path.join(root, '.git'))) {
    throw new Error('--repo-root must be an explicit Git worktree');
  }
  if (fs.realpathSync(git(root, ['rev-parse', '--show-toplevel']).toString('utf8').trim()) !== root) {
    throw new Error('--repo-root must be the Git top-level');
  }
  if (git(root, ['remote', 'get-url', 'origin']).toString('utf8').trim() !== pin.source.url ||
      git(root, ['rev-parse', `${pin.source.revision}^{commit}`]).toString('utf8').trim() !==
        pin.source.revision) {
    throw new Error('Source origin or locally available committed revision differs from pin');
  }
  if (source.meta?.repository?.url !== pin.source.url ||
      source.meta.repository.revision !== pin.source.revision ||
      source.diagram_type !== 'architecture' || source.meta.quality_profile !== 'showcase') {
    throw new Error('Authored graph must match the pinned repository, revision and showcase mode');
  }
  const cited = new Set(pin.source.extraCitedPaths);
  if (!source.components?.length) throw new Error('Graph must contain source-backed components');
  for (const node of source.components) {
    if (!node.sources?.length || node.sources.length > 3) {
      throw new Error(`${node.id}: require 1–3 committed source spans`);
    }
    for (const span of node.sources) {
      if (!Number.isSafeInteger(span.line) || !Number.isSafeInteger(span.end_line) ||
          span.line < 1 || span.end_line < span.line) {
        throw new Error(`${node.id}: require inclusive positive source lines`);
      }
      cited.add(span.path);
    }
  }
  // Do not reject unrelated/site-only dirty files or require HEAD to be the source pin.
  // Compare actual cited bytes, not index flags or Git status; finalize verifies committed spans.
  for (const name of cited) {
    if (typeof name !== 'string' || !name || path.posix.isAbsolute(name) ||
        name.split('/').some((part) => !part || part === '.' || part === '..') ||
        /[\\\x00-\x1f\x7f]/.test(name)) throw new Error('Unsafe cited source path');
    const file = path.join(root, name);
    if (!inside(root, fs.realpathSync(file)) || !fs.lstatSync(file).isFile()) {
      throw new Error(`Cited source must be a regular repository file: ${name}`);
    }
    const committed = git(root, ['cat-file', 'blob', `${pin.source.revision}:${name}`]);
    if (!fs.readFileSync(file).equals(committed)) {
      throw new Error(`Cited source differs from pinned committed bytes: ${name}`);
    }
  }
  return [...cited].sort();
}

function auditHtml(html, privateRoots) {
  const findings = [];
  const local = (value) => /^(?:data:|blob:|#)/i.test(value);
  for (const tag of html.matchAll(/<([a-z][\w:-]*)\b([^<>]*)>/gi)) {
    const name = tag[1].toLowerCase();
    for (const attr of tag[2].matchAll(/(?:^|\s)(src|srcset|poster|data|href|action)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/gi)) {
      const key = attr[1].toLowerCase();
      const value = attr[2] ?? attr[3] ?? attr[4];
      if (key === 'href' && name === 'a') continue; // Deliberate source navigation is not a runtime asset.
      if (key === 'data' && name !== 'object') continue;
      if (!local(value)) findings.push(`External/relative resource: ${name}.${key}`);
    }
  }
  const css = [
    ...Array.from(html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi), (m) => m[1]),
    ...Array.from(html.matchAll(/\sstyle\s*=\s*(?:"([^"]*)"|'([^']*)')/gi), (m) => m[1] ?? m[2]),
  ].join('\n');
  for (const match of css.matchAll(/url\(\s*['"]?([^)'"\s]+)/gi)) {
    if (!local(match[1])) findings.push('External/relative CSS resource');
  }
  if (/\b(?:fetch|XMLHttpRequest|WebSocket|EventSource|sendBeacon|importScripts)\s*\(|\bimport\s*\(|@import\b/.test(html)) {
    findings.push('Network API or CSS/module import token');
  }
  const decoded = html.replaceAll('\\/', '/').replace(/\\u002f/gi, '/');
  if (privateRoots.some((root) => decoded.includes(root)) || /\/(?:tmp|root|home)\//.test(decoded)) {
    findings.push('Absolute private path embedded in HTML');
  }
  if (!/<html\b[^>]*\blang=["']ru["']/i.test(html)) findings.push('Missing lang=ru');
  return { kind: 'static-offline-resource-audit', ok: findings.length === 0, findings };
}

function verifyReceipt(summary, receipt, candidateSha256, htmlSha256) {
  for (const report of [summary, receipt]) {
    if (report.ok !== true || report.status !== 'pass' || report.command !== 'finalize' ||
        report.type !== 'architecture' || report.quality !== 'showcase' ||
        report.specification?.sha256 !== candidateSha256 || report.artifact?.sha256 !== htmlSha256 ||
        report.browserCheck?.requested !== true || report.browserCheck.ran !== true) {
      throw new Error('Finalize did not produce a passing, hash-bound real-browser receipt');
    }
  }
  for (const stage of ['validate', 'deliver', 'check', 'browser-check']) {
    if (summary.gates?.[stage] !== 'pass' || receipt.stages?.[stage]?.status !== 'pass' ||
        receipt.stages[stage].exitCode !== 0) {
      throw new Error(`Finalize gate did not pass: ${stage}`);
    }
  }
  const delivered = receipt.stages.deliver.receipt;
  const checked = receipt.stages.check.receipt;
  if (receipt.update?.reason !== 'disabled' || delivered.update?.reason !== 'disabled' ||
       delivered.evidence?.verified !== true ||
       delivered.evidence.repository?.replace(/\.git$/, '') !== pin.source.url.replace(/\.git$/, '') ||
      delivered.evidence.revision !== pin.source.revision ||
      checked.provenance !== 'current' || checked.composition?.status !== 'pass') {
    throw new Error('Finalize must verify pinned source evidence/current provenance with updates disabled');
  }
}

async function main() {
  const usage = 'node docs/site/build-graph.cjs --tool-root PATH --repo-root PATH --out NEWDIR';
  const args = process.argv.slice(2);
  const options = {};
  for (let i = 0; i < args.length; i += 2) {
    const [key, value] = args.slice(i, i + 2);
    if (!['--tool-root', '--repo-root', '--out'].includes(key) || !value ||
        value.startsWith('--') || options[key]) throw new Error(usage);
    options[key] = path.resolve(value);
  }
  if (Object.keys(options).length !== 3) throw new Error(usage);
  const tool = fs.realpathSync(options['--tool-root']);
  const repo = fs.realpathSync(options['--repo-root']);
  // Resolve the existing parent first, including symlinks; mkdir is exclusive/non-recursive.
  const parent = fs.realpathSync(path.dirname(options['--out']));
  const out = path.join(parent, path.basename(options['--out']));
  if (!fs.statSync(parent).isDirectory()) throw new Error('Output parent must be an existing directory');
  if ([here, repo, tool].some((root) => inside(root, out))) {
    throw new Error('Output must be outside the authored, source and tool trees');
  }
  try {
    fs.lstatSync(out);
    throw new Error('Output must be NEW; existing directories/files/symlinks are never overwritten');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  if (packageTreeDigest(tool) !== pin.packageTreeSha256 ||
      readJson(path.join(tool, 'package.json')).version !== pin.packageVersion) {
    throw new Error('Tool package tree/version differs from the immutable pin');
  }
  const sourceBytes = fs.readFileSync(path.join(here, 'graphs/git-rs.source.json'));
  const source = JSON.parse(sourceBytes);
  const citedPaths = verifySource(repo, source);
  const base = readJson(path.join(tool, 'examples/locales/ru.json'));
  const extra = readJson(path.join(here, 'graphs/ru-extra.json'));
  if (Object.keys(extra).length !== 25 || Object.keys(extra).some((key) => Object.hasOwn(base, key))) {
    throw new Error('Require exactly 25 extra Russian keys, without replacing pinned base messages');
  }
  const translations = { ...base, ...extra };
  const localeBytes = Buffer.from(`${JSON.stringify(translations, null, 2)}\n`);
  const { validateTranslations } = await import(pathToFileURL(path.join(tool, 'renderers/shared/i18n.mjs')));
  const locale = validateTranslations(translations);
  if (Object.keys(translations).length !== 486 || locale.totalKeys !== pin.localeKeys ||
      locale.coveredKeys !== pin.localeKeys || locale.missingKeys.length ||
      locale.unknownKeys.length || locale.placeholderMismatches.length ||
      sha256(localeBytes) !== pin.localeSha256) {
    throw new Error('Require the pinned 486-key Russian catalog with exact placeholder coverage');
  }
  fs.mkdirSync(out, { mode: 0o700 });
  fs.mkdirSync(path.join(out, 'reports'), { mode: 0o700 });
  source.meta = { ...source.meta, locale: 'ru', translations, output: 'git-rs.html' };
  const candidateName = 'git-rs.archify.json';
  const htmlName = 'git-rs.html';
  writeJson(path.join(out, candidateName), source);
  const candidateSha256 = sha256(fs.readFileSync(path.join(out, candidateName)));
  // FIRST and only renderer invocation: finalize embeds validation and owns all delivery gates.
  const result = spawn(process.execPath, [
    path.join(tool, 'bin/archify.mjs'), 'finalize', 'architecture', candidateName, htmlName,
    '--repo-root', repo, '--quality', 'showcase', '--json',
  ], out);
  fs.writeFileSync(path.join(out, 'reports/finalize.stdout.json'), result.stdout || '', { flag: 'wx' });
  fs.writeFileSync(path.join(out, 'reports/finalize.stderr.log'), result.stderr || '', { flag: 'wx' });
  writeJson(path.join(out, 'reports/process.json'), {
    status: result.status, signal: result.signal, error: result.error?.message || null,
    timeoutMs: 150_000, maxBufferBytes: 16 * 1024 * 1024,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`Finalize failed; outputs preserved in ${out}: ${result.error?.message ||
      result.signal || result.status}`);
  }
  const summary = JSON.parse(result.stdout.toString('utf8'));
  const receipt = readJson(path.join(out, 'git-rs.finalize.json'));
  const htmlBytes = fs.readFileSync(path.join(out, htmlName));
  const htmlSha256 = sha256(htmlBytes);
  verifyReceipt(summary, receipt, candidateSha256, htmlSha256);
  if (sha256(fs.readFileSync(path.join(out, candidateName))) !== candidateSha256) {
    throw new Error('Generated candidate changed during finalize');
  }
  const audit = auditHtml(htmlBytes.toString('utf8'), [here, tool, repo, out]);
  writeJson(path.join(out, 'reports/offline-audit.json'), audit);
  if (!audit.ok) throw new Error(`HTML offline/privacy audit failed: ${audit.findings.join('; ')}`);
  fs.mkdirSync(path.join(out, 'licenses'), { mode: 0o700 });
  const licenses = pin.licenses.map((file) => {
    const name = `licenses/${path.basename(file)}`;
    fs.copyFileSync(path.join(tool, file), path.join(out, name), fs.constants.COPYFILE_EXCL);
    return { name, sha256: sha256(fs.readFileSync(path.join(out, name))) };
  });
  // Success marker is last. Failure paths retain artifacts/receipts but never a success manifest.
  writeJson(path.join(out, 'manifest.json'), {
    format: 'git-rs-archify-build/v1',
    revision: pin.source.revision,
    html: { name: htmlName, sha256: htmlSha256 },
    candidate: { name: candidateName, sha256: candidateSha256 },
    source: { url: pin.source.url, revision: pin.source.revision, citedPaths,
      candidatePath: 'docs/site/graphs/git-rs.source.json', sha256: sha256(sourceBytes) },
    tool: { repository: pin.repository, revision: pin.revision, version: pin.packageVersion,
      packageTreeSha256: pin.packageTreeSha256 },
    builder: { name: 'docs/site/build-graph.cjs', sha256: sha256(fs.readFileSync(__filename)) },
    pin: { name: 'docs/site/archify-pin.json', sha256: sha256(fs.readFileSync(pinFile)) },
    locale: { language: 'ru', keys: locale.coveredKeys, sha256: sha256(localeBytes) },
    licenses,
    checks: { finalize: 'pass', gates: summary.gates, sourceEvidence: 'verified',
      provenance: 'current', staticOfflineAudit: 'pass', visualReview: receipt.visualReview },
    receipts: { full: 'git-rs.finalize.json', summary: 'git-rs.finalize-summary.json',
      stdout: 'reports/finalize.stdout.json', offlineAudit: 'reports/offline-audit.json' },
  });
  console.log(`Graph build passed: ${path.join(out, 'manifest.json')}`);
}

if (require.main === module) main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});

module.exports = { verifyReceipt, auditHtml };
