# Fork-free architecture decision

Status: mandatory target. The current dependency graph still contains forks;
this decision is not a claim that their migration has been implemented or qualified.

## Decision

Use unmodified, maintained upstream libraries through supported public APIs.
There are no fork exceptions for missing functionality, performance instrumentation,
resource accounting or compatibility. Do not disguise a fork as vendored code,
an application implementation copied from library internals, a patched package,
or an external service carrying the same private dependency changes.

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
suspended traversal state or a mandatory persistent-mapping callback. Cache a
complete answer for an immutable target. Any unchanged-parent shortcut must prove
equivalence independently and preserve the normal computation path. Add an index
optimization only when a supported implementation can consume it and its measured
benefit justifies the application complexity.

## Dependency migration direction

- **Gitoxide:** start with its ordinary public operation APIs behind the annotation
  and object-source boundaries. Evaluate another maintained backend for an actual
  unsupported product capability; do not rebuild the current private hook surface.
  Qualify blame, physical packs, receive, history and backup in their real callers.
- **zstd:** retain useful compression and compatibility with existing raw/zstd
  artifacts. Use upstream bounded streaming and a qualified job/resource policy.
  Exact internal workspace estimates or inspection hooks are not independently
  required product features. Document any change in accepted frame policy.
- **Kubert:** evaluate the necessity of the advisory Kubernetes-election layer.
  PostgreSQL fencing already owns maintenance mutation authority. Simplify that
  coordination or use supported APIs without maintaining a lease-library fork.
  Ordinary cache warming does not acquire source-maintenance authority.
- **iconv:** evaluate safe public streaming conversion APIs in other upstream
  libraries and separate conversion semantics from runtime-attestation policy.
  Establish native behavior with independent encoding fixtures; do not replace
  missing APIs with application unsafe FFI or a renamed copy of the fork.

## Concrete public-API candidates

- Gitoxide's ordinary `gix_blame::file` is the first annotation candidate.
  Its object provider is public; persistent diff injection is not a required
  capability of the application boundary.
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

Current full revision pins, licenses and tests remain evidence until each caller
has a qualified replacement. Completion requires a resolved production dependency
graph with no fork source, patched crate or fork-backed fallback, along with the
affected behavioral and resource checks. Documentation alone does not satisfy
that completion gate.

The [execution plan](../vibe/fork-free-plan.md) uses these boundaries and treats
the current persisted-mapping WorkSet as a removable optimization, not a reason
to preserve private dependency APIs.
