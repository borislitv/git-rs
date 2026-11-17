# Architecture atlas

Open `index.html` directly in a browser. The site has no runtime dependencies,
external assets, upload endpoint, or telemetry. Alternatively, serve this
directory on loopback with `python3 -m http.server --bind 127.0.0.1`.
Source links point to the explicit public revision shown on the page; following
one of those links requires a network connection.

The interface is in Russian. The component graph supports pan/zoom, keyboard
navigation, search and request-path highlighting. Select a component to find related request steps,
or choose a flow and a step to inspect its I/O, checks, copies, limits and
resource descriptions. The public atlas describes code rather than asserting
production performance. Inclusive wall timers may overlap, configured
capacities are not RSS, and missing measurements are not zero.

The readiness and improvement tabs distinguish executed small-scenario success,
open large-workload blockers, protections, and actions with explicit conditions
and costs. The cache tab maps five data structures to three locations: shared
process RAM, per-job RAM, and one optional PostgreSQL/S3 derived-cache subsystem.
Each location links back to its actual component on the main graph. Fifteen
parameter groups retain their source-backed technical details. A cache
hit does not replace source authority or imply a hit in another layer. The graph
is a logical map of component transitions from request steps, not a timing trace.

The Resources tab accepts an optional local JSON file in the documented
`git-rs-architecture-evidence/v1` format. The file is read into the current
page's memory and is not sent to the server or saved in browser storage. The
page displays scope, revision and limitations with the measurements and warns
when revisions differ. Select Clear or close the tab to discard the data. Do not
commit private evidence files or publish them with the site.

Optional `elapsed_spans` entries contain `name`, nonnegative finite `seconds`
and `detail` (at most 20 spans per item). Their inclusive elapsed bars overlap;
they are not additive phases or CPU attribution. Historical CPU-clock self
samples remain separate from current-source elapsed measurements.

Optional `takeaways` holds up to eight bounded text strings per evidence item.
These are supplied interpretations, not automatically inferred diagnoses. Local
conclusions appear before raw profiles and are cleared together with the evidence;
the public source contains no private measurements or derived numerical claims.

## Allowlisted builds and local hosting

Node.js 20+ is sufficient; the builder has no package dependencies. The output
must be a new directory outside the repository, with an existing parent:

```sh
node docs/site/build.cjs --out /tmp/git-rs-atlas-public
node docs/site/build.cjs --out /tmp/git-rs-atlas-local --evidence /private/profile.json
python3 -m http.server 8767 --bind :: --directory /tmp/git-rs-atlas-local
```

The basic public build physically includes only nine static assets plus its manifest
and checksums. It never walks or copies neighboring evidence, tests or build
scripts. The explicit local build adds one validated, inert data script and
shows a local-profile notice; it is not suitable for public hosting. Measurements
appear automatically in Resources and on their associated request step. Clearing
them changes page memory only; reloading restores the explicitly bundled profile.

For access by the SSH host address, `--bind ::` serves IPv4 and IPv6 on port
8767. Serve only the built directory. A transient systemd unit may keep this
preview alive independently of the agent; it does not imply reboot persistence.
Verify both address families and the actual served profile before delivery.

The current preview uses `git-rs-architecture-20261117-a2.service` on the SSH
host, port 8767. Inspect it with `systemctl status` and stop this exact unit
with `systemctl stop git-rs-architecture-20261117-a2.service` when no longer needed.
The server remains independent of the agent session, not of a host reboot.

```sh
node --test docs/site/tests/*.test.cjs
```

## Optional Archify companion

The main graph works without Archify. An additional, independently explorable
component map uses the pinned upstream tool and public committed source spans.
`archify-pin.json` records tool/source/package-tree/locale identity. Nothing is
installed, downloaded or updated by this recipe:

```sh
ARCHIFY_CHROME=/path/to/existing/chromium node docs/site/build-graph.cjs \
  --tool-root /path/to/pinned/archify-tool --repo-root /path/to/git-rs \
  --out /tmp/git-rs-archify
node docs/site/build.cjs --out /tmp/git-rs-atlas-with-graph --graph /tmp/git-rs-archify
```

The builder checks cited working files against pinned committed blobs, merges
the complete Russian locale, then runs source-aware `finalize` including its
real-browser gate. It retains failed outputs; a success manifest is written
only after all gates, source/provenance/hash and offline-resource checks pass.
The static site builder verifies that manifest against the authored candidate
and copies only the HTML plus its five required license/notice files. Generated
HTML, receipts, tool code and internal observations are not committed.

The companion is linked only in an explicitly built distribution. Serving the
authored directory directly (including Pages) uses the main graph without
requesting a missing optional companion. Archify is a component model, not an
execution simulator or a measured timing diagram.

## Browser verification

The optional browser test requires Node.js, `playwright-core` and a compatible
Chromium executable. Keep the test dependency outside the repository:

```sh
npm install --prefix /tmp/git-rs-site-test --no-save --ignore-scripts --no-audit --no-fund playwright-core@1.65.1
NODE_PATH=/tmp/git-rs-site-test/node_modules CHROMIUM_PATH=/path/to/chromium node docs/site/tests/browser.cjs
```

The test uses synthetic evidence and an ephemeral loopback HTTP server. It
checks desktop HTTP and mobile `file://` navigation, all request steps,
responsive overflow, keyboard and history navigation, local evidence import,
clearing and rejection, text rendering, and unexpected external requests. When
the build includes Archify, the test also opens its standalone Russian SVG map.

`BASE_URL` checks an already served build (including a Pages subdirectory) in
desktop and mobile browsers. `SITE_DIRECTORY` selects a built directory for
local HTTP/file tests; `SCREENSHOT_DIRECTORY` selects an existing private report
directory. Do not set `EVIDENCE_PATH` when checking a public HTTPS site. Only
synthetic evidence is imported by the public-site check.
