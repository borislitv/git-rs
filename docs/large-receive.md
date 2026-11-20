# Opt-in native HTTP bootstrap receive

`baseline-v1` (the default) keeps normal Smart HTTP and SSH receive limits and
normalization. `large-bootstrap-v1` selects a separate path for the initial
Smart HTTP push into an authorized snapshot with **no refs and no packs**. To
enable it on `git-rs serve`, set `GIT_RS_RECEIVE_RESOURCE_PROFILE=large-bootstrap-v1`.
`serve` requires a Smart HTTP listener and rejects a source cache over 64 MiB
with this setting. Set `GIT_RS_BLAME_CONCURRENCY=1` and
`GIT_RS_BLAME_WAITERS=0`; the constructor rejects other values for the large
profile. Blame can keep its baseline resource profile. Use a 64-bit runtime.

The native client needs no custom protocol or catalog loader. For example, from
a synthetic repository with the desired refs:

```sh
git -c http.postBuffer=65536 push https://git.example.invalid/team/project.git \
  refs/heads/main:refs/heads/main
```

The small client buffer allows streaming HTTP/1.1 chunks. The service accepts
identity bodies framed by either Content-Length or chunked transfer. The gateway
must admit the body size and timeouts without buffering the whole upload. Select
an appropriate finite `GIT_RS_OPERATION_SECONDS` (at most 3600); framing, indexing,
graph validation, S3, SQL and status share the original absolute deadline. The
large receive PUT may use the remainder of that deadline rather than the baseline
110-second per-request cap. Use an S3 endpoint that accepts a signed single PUT
at this profile's limit. Return the listener to baseline after bootstrap for
normal updates. Packed bootstrap does not support gzip, `REF_DELTA`, thin packs,
duplicate object IDs, or extending existing history; it supports native full
objects and backward `OFS_DELTA` entries only. Baseline behavior is unchanged
outside this profile, including SSH.

| Resource | Large bootstrap bound |
| --- | ---: |
| Framed HTTP body including commands | 4,608 MiB |
| Simultaneous receive/job/result owners on the shared pool | 1 |
| Live charged capacity, including old/new buffer growth overlap | 10 GiB |
| Physical incoming entries / complete graph objects | 600,000 / 600,000 |
| Aggregate inspected edges / physical object reads | 64,000,000 / 64,000,000 |
| Aggregate reconstructed bytes / semantic re-read bytes | 8 TiB / 8 TiB |
| Decoder work | 2,000,000,000 controlled decoder checkpoints across both passes |
| Inventory resolution | At most 4 scoped lanes under the one job permit |
| Retained branched ancestors | At most 96 MiB per lane, with all transient buffers separately admitted |
| Physical object / per-chain decode workspace / depth | 128 MiB / 256 MiB / 64 |
| Commit or tag headers | 64 KiB |

The 256 MiB receive decoder limit is not the blame source's cumulative
expanded-chain limit. The admitted pack decoder charges the instruction sum
and bounds its planned working buffer (two maximum intermediate bodies plus
the instruction sum). Intermediate outputs may reuse that buffer. The 8 TiB
inventory counter sums the **final** result of each physical entry, not every
intermediate reconstruction across entries. Controlled decoder checkpoints and
the operation deadline independently bound that repeated work. The default
operation deadline stays 120 seconds; a planned large transfer must explicitly
select an appropriate finite deadline.

The 10 GiB is charged live Rust backing, not an RSS promise. At the final
chunked growth, up to 4 GiB old plus 4.5 GiB new pack backing can coexist; the
old buffer is gone before decode/index/graph storage is created. On a 16 GiB
host budget the other 6 GiB independently for allocator retention, transport,
shared caches, kernel, and colocated services. Keep shared caches small (for
example `GIT_RS_CACHE_BYTES=67108864`) and do not run other heavy jobs or a
second server on that host. Measure host/cgroup peak, not just the job budget.

Commands and pack header are checked before bulk allocation. Terminal HTTP
framing is required before starting CPU validation. One admitted, compressed
pack backing survives from HTTP intake through staged S3 upload. The server
checks the collision-detecting SHA-1 checksum and object IDs, inflates all
physical entries with bounded maintained zlib/pack decoding, checks that each
OFS base is an earlier physical entry start, and computes a v2 index from the
exact entry bytes and offsets. For a delta, the framing cursor comes from exact
zlib consumption of its physical instructions, not a decoder outcome summarizing
the delta chain. The semantic pass visits every physical record, including
unreachable objects, checks every typed edge and root, and checks `.gitmodules`
in filename context. Ordinary blob bodies are not decoded a second time; each
distinct non-blob is inspected on its first typed visit. Inventory first frames
the original pack and proves each chain plan, then resolves the resulting
backward-OFS forest, largest independent roots first. A child borrows its
verified parent, including when their physical ordinals are far apart. A linear
edge hands off the body without keeping its ancestors; branch retention is
limited to 96 MiB per lane and otherwise rematerializes using two bodies. The
chain plan still bounds depth, instructions, largest intermediate body, and
workspace, including fallback. All lane stacks, codec workspaces, metadata,
and buffers share the same job budget and all lanes join before graph validation.
The public Gitoxide tree traversal was not used because its traversal does not
give the application this aggregate admission, full-chain policy and shared
checkpoint control; framing and reconstruction still use Gitoxide public
entry/zlib/delta APIs. The
full ordered command projection is compared before joining the existing source
fences, staged PUTs, and one authoritative SQL commit. No scratch pack, native
server fallback, SQL import, or partial publication is used.

## Closed telemetry and source inventory

`/metrics` exposes `git_rs_receive_bootstrap_stage_total` with the fixed stages
`admission`, `source`, `body`, `inventory`, `graph`, `index`, `upload`, `publish`,
and `status`, and outcomes `completed`/`failed`. The matching `stage_active`
gauge shows work still in progress. Stage durations accumulate in
`git_rs_receive_bootstrap_stage_seconds_sum`. `publish/completed` means SQL
confirmed publication; `publish/failed` means it was not confirmed and does not
by itself distinguish rollback from an unknown result. `status/completed` means
status framing and flush succeeded, even if the protocol response was negative.
Git's empty HTTP probe can complete intake and status without publishing. The
`pack_bytes`, `objects`, `logical_bytes`, and `decoder_checkpoints` totals cover
requests that finished physical inventory. No identifiers, body material,
credentials, or formatted dependency errors are emitted by these metrics.
Physical inventory additionally exposes closed stages `inventory_checksum`,
`inventory_entries`, and `inventory_sort`. Numeric
`git_rs_receive_bootstrap_inventory_progress_total` counters report finalized
entry batches. Forest resolution reports approximately every 100 ms and at
join, including actual charged checkpoints on failure. Verified object counts
follow resolution, not physical ordinal; durable index offsets and the core
physical inventory remain in original pack order.
The inventory API also returns typed limit errors and counts reuse hits and
avoided ancestor bytes. Avoided bytes are derived from the proven chain
plans; compare actual decoder checkpoints with reuse off and on to measure
the work reduction. The inventory API also reports independent root count,
lane count, largest root by logical bytes, and peak retained branch bytes per
lane for synthetic measurement. One dominant root is not subdivided, so
intra-job parallelism is not proof of a fourfold speedup. Graph traversal and
framing remain serial. Measure representative root skew, actual checkpoints,
wall/CPU, and RSS before attempting another full push.

The application composition is in `src/receive/{profile,bootstrap}.rs`. The exact
inventory/index adapter is `crates/git-core/src/receive/packed.rs`; graph semantics
stay in `crates/git-core/src/receive/{walk,semantics}.rs`. The existing pinned
source inventory in `docs/gitoxide-source.json` already covers the admitted pack
decoder, pack memory seam, maintained index writer, and controlled zlib hook.

Qualification is separate from this implementation. The optional
`GIT_RS_TEST_LARGE_RECEIVE=1` lane of the existing receive integration fixture
generates a synthetic native repack above the baseline count cap, pushes it to
an empty repository, checks the exact published ref and pack object inventory,
and compares the uploaded v2 index to native `git index-pack --strict`. It is
not a multigigabyte or private-history resource qualification. For the latter,
use an actual push into an empty target, record the emitted body bytes, physical
count, successful complete ref batch and repository inventory, phase durations,
and host peak memory. A pack can still fail an object/chain/edge/decoder limit,
strict semantic validation, deadline, or a gateway/S3 bound even when its
compressed bytes and object count fit. No staged fast-forward substitute or
cross-request closure evidence is implemented. Smaller native pushes alone do
not solve a graph-limit failure: a later receive still walks the whole closure
of its proposed new root. An old ref in the catalog is not proof that its
reachable graph passed the same validation policy under a live storage epoch.
Several TiB of logical output can still make the single worker and one-hour
absolute operation deadline the binding limit. For each logical TiB, hashing
once within an hour requires about 291 MiB/s of sustained throughput, before
delta reconstruction, graph validation, and transfer. Raising the logical byte
counter does not demonstrate that throughput.
