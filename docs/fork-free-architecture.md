# Dependency architecture decision

Status: the pinned Gitoxide fork is the selected blame backend. Migrations for
other callers still require independent qualification.

## Decision

Use maintained upstream libraries through supported public APIs by default.
Maintain one explicit exception for `NativeRootV1`: the owned Gitoxide fork,
currently pinned at `d80e8c8f13952fef55fb8a387e64c9436537cecf`. The production
path at that pin matches full Git 2.56 porcelain on both independent-root and
same-path-before-rename merges. The tested upstream candidates each fail one
of those cases. Keep the existing implementation and qualify focused changes
there; do not create a second attribution engine in the application. Record
the exact pin, its upstream base and patch scope, and rerun native, resource
and performance gates on every changed producer.

A missing hook is first evidence that the application boundary or an internal
requirement needs reconsideration. Prefer, in order:

1. Remove an unnecessary internal requirement or optional optimization.
2. Express the operation with the library's existing public API.
3. Select another maintained upstream implementation.
4. Add a small application-owned abstraction with independently testable inputs,
   outputs and resource ownership.

Do not make release depend on an upstream project accepting an extension.
Actual user-visible requirements still need explicit verification. A changed
behavior must be documented as a changed contract, not hidden by relaxing a test
and calling the result compatible.

## Application boundaries

These are responsibility boundaries, not a request for a general plugin framework.

| Boundary | Application responsibility | Dependency responsibility |
| --- | --- | --- |
| Complete annotation backend | Authorized immutable target, fixed options, complete output and error conversion | Ordinary supported blame/object APIs |
| Job executor | Admission, effective CPU/memory limits, finite lifetime, cancellation, termination/drain and ownership of results | Execute an ordinary computation without allocator or inner-loop callbacks |
| Artifact store and codec | Stable logical keys, access checks, integrity, bounded input/output and complete publication | Supported compression/streaming APIs |
| Commit text conversion | Explicit encoding semantics, bounded operation and atomic result/failure | Supported safe conversion API of a maintained implementation |
| Warm reconciler | Revisit configured targets after restart, resolve current authorized tips, coalesce work and publish complete results | The same qualified annotation backend and artifact APIs as foreground work |

Resource protection belongs at a qualified job boundary when a library cannot
cooperatively expose individual allocations or interruption points. Moving work
to a thread does not provide a memory or cancellation boundary. Preserve source
holds and admission until actual completion, termination and I/O drain.

The annotation interface must not expose diff interners, allocation events,
suspended traversal state or a mandatory persistent-mapping callback at the
serving boundary. The fork may use admitted request-local hooks internally. Cache
a complete answer for an immutable target. Any unchanged-parent shortcut must prove
equivalence independently and preserve the normal computation path. Add an index
optimization only when a supported implementation can consume it and its measured
benefit justifies the application complexity.

## Dependency migration direction

- **Gitoxide:** keep the qualified owned blame backend. Use the standalone
  upstream adapters to qualify non-blame callers (physical packs, receive, history
  and backup). Treat current whole-workspace coupling as migration debt; each
  replacement must pass its real caller's native and resource gates.
- **zstd:** retain useful compression and compatibility with existing raw/zstd
  artifacts. Use upstream bounded streaming and a qualified job/resource policy.
  Exact internal workspace estimates or inspection hooks are not independently
  required product features. Document any change in accepted frame policy.
- **Kubert:** use the supported typed Kubernetes client, preserving the named
  Lease and its takeover and readiness contracts. PostgreSQL fencing owns
  maintenance mutation authority. Cache warming does not acquire that authority.
- **iconv:** evaluate safe public streaming conversion APIs in other upstream
  libraries and separate conversion semantics from runtime-attestation policy.
  Establish native behavior with independent encoding fixtures; do not replace
  missing APIs with application unsafe FFI or a renamed copy of the fork.

## Concrete public-API candidates

- Gitoxide's ordinary `gix_blame::file` and libgit2 are retained as standalone
  comparison probes. See [the measured merge failures](../tests/upstream_probe/README.md).
- `gix_pack::data::File::from_data`, `with_alloc_limit_bytes` and `decode_entry`
  provide a memory-backed decoder with an external `ResolvedBase::OutOfPack`
  resolver. Test a bounded in-memory entry adapter using the public pack-format
  APIs; do not assume its full-slice `FileData` is a lazy range provider.
- `gio::CharsetConverter` with `ConverterExtManual::convert` accepts caller-owned
  byte buffers. The underlying converter distinguishes ordinary conversion from
  explicit flush/end-of-input flags. This is a candidate for preserving stateful
  no-final-flush conversion without modifying an iconv crate. It adds GLib/GIO
  runtime dependencies and still needs full native-encoding qualification.

Sources inspected: [Gitoxide public decoder](https://github.com/GitoxideLabs/gitoxide/blob/b8e1eb2a3deaf67d82bdfec0ddad43b693639d8b/gix-pack/src/data/file/decode/entry.rs),
[GIO safe conversion API](https://github.com/gtk-rs/gtk-rs-core/blob/0.21.2/gio/src/converter.rs),
[GIO charset implementation](https://github.com/GNOME/glib/blob/2.86.1/gio/gcharsetconverter.c).
API availability is established by source inspection, not runtime qualification.

Completion requires a resolved production dependency graph that allows only the
recorded blame fork and its required closure. The source check must reject
undeclared patches and other fork sources. The dependency graph does not by
itself prove which callers use that closure or qualify runtime behavior.

The [execution plan](../vibe/fork-free-plan.md) uses these boundaries and treats
the current persisted-mapping WorkSet as a removable optimization, not a reason
to preserve private dependency APIs.
