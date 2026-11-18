# Architecture decision: cold blame, incremental blame and push

Scope: `NativeRootV1` on a changed literal path after a serving restart. The
decision concerns the read path and its tests; `receive-pack` has its own
publication contract. The representative history is a 20,000-commit base and
two requested tips at cumulative distances 1 and 100.

## What the observations establish

| Environment | Observation | Scope |
| --- | --- | --- |
| Earlier Kubernetes chart test | Two serving pods, native SSH/HTTP push and clone, history, cold restart, upgrade and leader replacement. | Two commits. The [chart test](https://github.com/borislitv/git-rs/blob/77ef7957fc16cca900f657ed4c814d9faacaf244/tests/kubernetes.py) did not request blame. |
| Earlier [cache matrix](https://github.com/borislitv/git-rs/blob/77ef7957fc16cca900f657ed4c814d9faacaf244/tests/cache_matrix.py) | HTTP blame matched native bytes and tested cross-process and restart hits. | The serving processes ran in containers; Kubernetes provided the maintenance Lease. This tested a small DAG. |
| Debug 20k HTTP gate, [`dd6aa93`](https://github.com/borislitv/git-rs/actions/runs/44255714735/job/134645051391) | The base request exceeded 120 s. Source acquisition, WorkSet preparation and the body claim completed in 3.5 s. | No completed-work observation. This observation is after the final checkpoint flush, so it cannot distinguish attribution from flush. |
| Optimized GET, [`2c5ca37` PR](https://github.com/borislitv/git-rs/actions/runs/44261018668/job/134660853089) and [push job](https://github.com/borislitv/git-rs/actions/runs/44261011345/job/134661507550) | Cold 20k base matched native bytes; `Complete` at 21.36 and 22.36 s from fixture tracing start. On the eight-commit case +1/+100 had zero attribution walks; the wrong signing key used ordinary blame. | Both 20k runs stopped at the **native +1 push** with curl 52, before the +1 GET. A completed base does not alone prove a published seed. |
| Matched release GET and receive, `2f46616` [push job](https://github.com/borislitv/git-rs/actions/runs/44267373656/job/134683560495) / [PR job](https://github.com/borislitv/git-rs/actions/runs/44267385530/job/134683016859) | Both 20k contracts passed. First GET after restart: 223/346 ms at +1 and 430/969 ms at cumulative +100 (push/PR job order). Both matched full native bytes, hit the seed, checked 1/100 suffix edges and performed zero attribution visits or new mappings. | Native HTTP pushed +1; +99 more used pack/catalog publication. These timings measure the GET, not the push. The short wrong-key control also passed. |

These are individual observations, not a latency distribution. At `2c5ca37`
the large GET used a release executable and a 64 MiB source cache. Its push
used a different executable: `cargo build --example blame-http-fixture`
without `--release`, with the fixture's 1 MiB default. The range store can
read 4 MiB blocks; a 1 MiB cache cannot retain one full block. The mismatch is established;
the reason the server closed the push connection is not established by curl.

## Decisions

**AD-1 — One attribution engine.** The owned Gitoxide pin owns the traversal,
merge/rename selection, previous paths and native group semantics. Git 2.56 is
the external byte oracle. [Gitaly's `RawBlame`](https://gitlab.com/gitlab-org/gitaly/-/blob/179b2d6cc66eac231ffacd1f6441acc79bd2295d/internal/gitaly/service/commit/raw_blame.go)
likewise delegates attribution to one engine and streams its result. Its local
`git blame -p` is not the serving or wire contract here. Its [path-aware commit
graph](https://gitlab.com/gitlab-org/gitaly/-/blob/179b2d6cc66eac231ffacd1f6441acc79bd2295d/internal/git/housekeeping/commit_graph.go)
is a housekeeping accelerator, not a completed attribution.

**AD-2 — Use completed results at the read boundary.** The source owner proves
current permission, reachability and reader holds. Then try the exact body, the
unchanged immediate parent, and an authenticated base plus a verified bounded
suffix; otherwise run ordinary blame. An edge mapping saves a diff, not the
walk to a surviving line's origin. A generation-scoped WorkSet is useful for
some retries but is not evidence for a different tip. Preserve immutable key
scope, MAC, physical integrity, admission and output/publication fences.

**AD-3 — Proposed: produce the certificate on the first traversal.** Today
`certify_replacement_seed` walks the linear commit/tree chain again *after*
completed attribution. Have the owned engine accumulate a bounded witness
from the edges it already verifies. If a shortcut does not provide sufficient
evidence, decline the certificate. Check the ordered native groups and provenance
against the renderer before signing. Version the proof; do not re-sign a cache
hit. Initially retain the fixed-coordinate, single-file, sole-parent scope.
Validate the whole request, including discovery: the current geometric batches
can read below a seed that falls in the middle of a batch. A zero `commits_traversed`
counter refers to attribution, not to candidate or reachability reads.

**AD-4 — Qualify publication and reads independently.** A confirmed push
publishes its exact sealed batch through authoritative PG/S3; caching and
warming are optional. An empty HTTP reply does not say whether SQL committed.
Reconcile the exact journaled batch before reporting publication. For tests,
use the same release build and source-cache budget for GET and receive. Keep
a catalog-advance serving probe and a separate genuine native receive gate;
a successful catalog advance cannot qualify receive. A production warmer, if
needed, reconciles protected targets outside the push transaction with separate
capacity and fresh read authority. Retain named Lease election and PG fencing.

## Acceptance and next experiment

1. Measure cold base, seed publication, first GET at +1/+100 after restart,
   and native push separately. Keep 120 s per GET, 90 s for the receiving server
   and a 95 s client bound. Never recover a timeout by serving a partial body.
2. Require complete Git 2.56 bytes, `ReplacementSeedHit`, exactly 1/100 suffix
   edges, zero attribution visits/new mappings, and no commit reads below the
   selected base for qualifying tip hits. Include a non-batch-end distance and
   verify the read guard covers discovery as well as composition. Keep wrong-key,
   +101, merge/rename, duplicate-line and revoked-source controls.
3. On a receive failure, distinguish pack preparation, graph validation, upload,
   SQL confirmation and final report/flush before choosing a fix. On a slow
   cold GET, split attribution, checkpoint flush, seed proof and publication;
   compare paired runs before changing WorkSet policy or adding a wider index.
4. For deployment parity, replay a matched release fixture on two pods: pod A
   builds the base, B serves the changed tip, replace B and repeat. Pin the ELF,
   object set, effective CPU/RAM, PG/S3, key/profile and native client. Compare
   bytes and work as well as request time. Leave general-DAG incremental reuse
   and a universal per-path index deferred until a measured workload needs them.
