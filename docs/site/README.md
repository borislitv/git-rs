# Architecture atlas

Open `index.html` directly in a browser. The site has no runtime dependencies,
external assets, upload endpoint, or telemetry. Alternatively, serve this
directory on loopback with `python3 -m http.server --bind 127.0.0.1`.
Source links point to the explicit public revision shown on the page; following
one of those links requires a network connection.

The interface is in Russian. Select a component to find related request steps,
or choose a flow and a step to inspect its I/O, checks, copies, limits and
resource descriptions. The public atlas describes code rather than asserting
production performance. Inclusive wall timers may overlap, configured
capacities are not RSS, and missing measurements are not zero.

The Resources tab accepts an optional local JSON file in the documented
`git-rs-architecture-evidence/v1` format. The file is read into the current
page's memory and is not sent to the server or saved in browser storage. The
page displays scope, revision and limitations with the measurements and warns
when revisions differ. Select Clear or close the tab to discard the data. Do not
commit private evidence files or publish them with the site.

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
clearing and rejection, text rendering, and unexpected external requests.
