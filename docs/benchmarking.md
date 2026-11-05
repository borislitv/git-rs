# Comparative benchmark methodology

Compare supported configurations of native Git, GitLab and git-rs on an isolated environment.
Keep private repositories, credentials, exports and raw traces outside the public source tree and
Docker build context. Do not enable CI, LFS smudging or client filters merely by importing data.

## Workload and correctness

1. Fix the same real or synthetic source commits and refs for every backend. Record file/object
   counts, logical bytes and reference-pack sizes; distinguish these from measured wire bytes.
2. Replay content-changing snapshots without empty-commit inflation. Preserve executable bits,
   symlink targets and exact file bytes, overriding client attributes when required by the workload.
3. Measure initial new-pack push, incremental new-pack push, incremental fetch, no-op fetch, warm
   full clone and process-cold clone. Use fresh repositories for each import and ingestion trial:
   moving a ref to already-present objects is a different operation.
4. Verify exact refs and commit/tree identities and perform independent readback and `git fsck`.
   Verify unchanged worker cycles create neither commits nor pushes.
5. Separate shared export/replay, synchronization/index, commit, network preparation and push
   phases. Measure the original publication worker as well as individual Git operations.

## Fairness and measurement

- Apply comparable aggregate CPU/RAM budgets. Include GitLab's whole stack and git-rs's serving
  process, SSH, PostgreSQL and S3 implementation. Record individual static component caps too.
- Right-size GitLab workers to the container budget: automatic sizing may see host CPU count
  rather than the intended cgroup limit. Record effective workers/threads and retain failed sizing
  cohorts separately. Do not change Gitaly/durability settings without identifying a new profile.
- Use supported Gitaly local storage. Record whether S3 is a production RGW or a local fixture;
  a POSIX-backed fixture cannot establish Ceph performance or HA equivalence.
- Fix client/tool/server versions, identities, protocols, SSH host-key verification and TLS policy.
  HTTP versus SSH and different client credential helpers are distinct profiles.
- Use concurrency 1, 2 and 4 with repeated trials, rotating backend order. Keep errors/timeouts and
  setup failures; never discard slow successful observations or retry scored operations until green.
- Report group completion and per-client latency, median, observed tail/max, throughput and errors.
  Five repetitions provide only a small sample: nearest-rank p95 equals the maximum, not an SLO.
- Sample whole-container CPU counters and aggregate memory around operations. A maximum sampled
  sum is not an exact simultaneous peak or a minimum required memory allocation.
- Define cache state precisely. Restarting Gitaly or git-rs clears selected process caches, not
  kernel page cache, database buffers or storage caches. Never flush a shared host's caches.

## Interpretation

Initial ingestion, tiny incremental publication, no-op checks and full reads can favor different
architectures. Report workload-specific differences rather than an overall speedup claim. Keep
the deployment footprint, durability model and tuning costs alongside latency. A replay of fixed
exports establishes that replay's behavior; it is not a measurement of an existing live backup
service, its retained archive history or its production storage failure domains.

Relevant official references:

- [Gitaly storage](https://docs.gitlab.com/administration/gitaly/).
- [Gitaly pack-objects cache](https://docs.gitlab.com/administration/gitaly/configure_gitaly/#pack-objects-cache).
- [Gitaly concurrency limiting](https://docs.gitlab.com/administration/gitaly/concurrency_limiting/).
- [GitLab memory-constrained environments](https://docs.gitlab.com/omnibus/settings/memory_constrained_envs/).
