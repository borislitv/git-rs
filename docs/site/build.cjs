'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const evidence = require('./evidence.js');

const assets = Object.freeze([
  'index.html', 'style.css', 'architecture-data.js', 'guide-data.js', 'app.js',
  'graph.js', 'graph.css', 'evidence.js', 'favicon.svg',
]);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

function buildSite({ output, evidencePath, graphPath } = {}) {
  if (!output) throw new Error('An output directory is required.');
  const source = fs.realpathSync(__dirname);
  const destination = path.join(fs.realpathSync(path.dirname(path.resolve(output))), path.basename(output));
  const relative = path.relative(path.resolve(__dirname, '../..'), destination);
  if (!relative || (!relative.startsWith(`..${path.sep}`) && relative !== '..')) {
    throw new Error('Output must be outside the repository.');
  }
  let local;
  if (evidencePath) {
    const fd = fs.openSync(evidencePath, 'r');
    try {
      if (!fs.fstatSync(fd).isFile() || fs.fstatSync(fd).size > evidence.MAX_BYTES) {
        throw new Error('Evidence must be a regular JSON file no larger than 2 MiB.');
      }
      const buffer = Buffer.alloc(evidence.MAX_BYTES + 1);
      let count = 0;
      while (count < buffer.length) {
        const read = fs.readSync(fd, buffer, count, buffer.length - count, null);
        if (!read) break;
        count += read;
      }
      if (count > evidence.MAX_BYTES) throw new Error('Evidence exceeds 2 MiB.');
      local = { format: 'git-rs-architecture-evidence/v1', items: evidence.validate(JSON.parse(buffer.subarray(0, count))) };
    } finally { fs.closeSync(fd); }
  }
  // Read every allowed source before creating output; private siblings are never walked.
  const files = new Map(assets.map((name) => [name, fs.readFileSync(path.join(source, name))]));
  if (graphPath) {
    const graph = JSON.parse(fs.readFileSync(path.join(graphPath, 'manifest.json'), 'utf8'));
    const pin = JSON.parse(fs.readFileSync(path.join(source, 'archify-pin.json'), 'utf8'));
    const expectedSource = sha256(fs.readFileSync(path.join(source, 'graphs/git-rs.source.json')));
    if (graph.format !== 'git-rs-archify-build/v1' || graph.revision !== pin.source.revision
      || graph.source?.sha256 !== expectedSource || graph.tool?.packageTreeSha256 !== pin.packageTreeSha256
      || graph.checks?.finalize !== 'pass' || graph.checks?.staticOfflineAudit !== 'pass'
      || graph.html?.name !== 'git-rs.html') throw new Error('Graph is not a passing current authored-source build.');
    const names = pin.licenses.map((name) => `licenses/${path.basename(name)}`);
    if (JSON.stringify(graph.licenses?.map((entry) => entry.name).sort()) !== JSON.stringify([...names].sort())) {
      throw new Error('Graph license manifest is incomplete.');
    }
    for (const entry of [graph.html, ...graph.licenses]) {
      const bytes = fs.readFileSync(path.join(graphPath, entry.name));
      if (sha256(bytes) !== entry.sha256) throw new Error(`Graph hash mismatch: ${entry.name}`);
      files.set(`graphs/${entry.name}`, bytes);
    }
    const html = files.get('index.html').toString('utf8');
    files.set('index.html', Buffer.from(html.replace('<!-- ARCHIFY_COMPANION -->',
      '<p class="profile-notice"><a id="archify-link" href="graphs/git-rs.html" target="_blank" rel="noopener noreferrer">Открыть расширенную схему Archify ↗</a> · <a href="graphs/licenses/NOTICE">лицензии</a></p>')));
  }
  if (local) {
    const serialized = JSON.stringify(local).replace(/</g, '\\u003c')
      .replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
    files.set('local-evidence.js', Buffer.from(`globalThis.GIT_RS_LOCAL_EVIDENCE = ${serialized};\n`));
    const html = files.get('index.html').toString('utf8');
    const marker = '  <script src="app.js" defer></script>';
    if (!html.includes(marker)) throw new Error('Evidence script insertion point not found.');
    files.set('index.html', Buffer.from(html.replace(marker,
      `  <script src="local-evidence.js" defer></script>\n${marker}`)));
  }
  const manifest = {
    format: 'git-rs-architecture-build/v1',
    localEvidence: Boolean(local),
    archifyCompanion: Boolean(graphPath),
    files: [...files].map(([name, bytes]) => ({ name, bytes: bytes.length, sha256: sha256(bytes) })),
  };
  files.set('manifest.json', Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`));
  const checksums = [...files].map(([name, bytes]) => `${sha256(bytes)}  ${name}`).join('\n');
  files.set('SHA256SUMS', Buffer.from(`${checksums}\n`));
  fs.mkdirSync(destination, { mode: 0o700 });
  for (const [name, bytes] of files) {
    if (path.dirname(name) !== '.') fs.mkdirSync(path.dirname(path.join(destination, name)), { recursive: true });
    fs.writeFileSync(path.join(destination, name), bytes, { flag: 'wx' });
  }
  return manifest;
}

if (require.main === module) {
  try {
    const options = {};
    const args = process.argv.slice(2);
    for (let i = 0; i < args.length; i += 2) {
      const key = { '--out': 'output', '--evidence': 'evidencePath', '--graph': 'graphPath' }[args[i]];
      if (!key || !args[i + 1] || options[key]) throw new Error('Usage: node build.cjs --out NEW_DIRECTORY [--evidence PRIVATE_JSON] [--graph VERIFIED_GRAPH_DIR]');
      options[key] = args[i + 1];
    }
    const manifest = buildSite(options);
    console.log(`Built ${manifest.files.length} allowlisted assets; local evidence: ${manifest.localEvidence}.`);
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

module.exports = { buildSite, assets };
