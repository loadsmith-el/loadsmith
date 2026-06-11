# Rejected Ideas

A running log of architectural ideas that came up, were considered, and were
**not** pursued — with the reasoning. The point isn't to shame the idea; it's
so that when it resurfaces (and good ideas often do), we don't re-spend the
time re-deriving why it doesn't fit, unless something material has changed.

Each entry: the idea, why it was rejected, and (if relevant) what would have
to change for it to be worth reconsidering.

---

## Unified read/write driver per plugin family

**Idea:** Instead of separate `SourcePlugin` and `DestinationPlugin`
implementations per backend (e.g. `plugins/postgres/src/source.rs` and
`destination.rs`), have a single "driver" per backend that knows how to both
read and write, implementing one combined trait.

**Why not:** The low-level shared surface — connection/TLS setup, type
mapping, wire format — is *already* factored out (`conn.rs`, `types.rs`,
`copy.rs` in the postgres plugin) and reused by both sides. What's left in
`source.rs` and `destination.rs` isn't the same operation mirrored; it's
genuinely different state machines: the source manages a cursor, watermark,
and transaction isolation for incremental reads, while the destination manages
staging tables, `MERGE`, and commit-mode semantics. A combined trait would mean
half its methods are meaningless on either side (`resume_from`/
`current_watermark` for a destination, `finalize`/staged-merge for a source).

It would also fight the core's protocol design: source and destination run as
separate plugin processes connected through separate pipes, by design (see
[Data Flow](./data-flow.md)) — collapsing the plugin-side abstraction doesn't
match a core that deliberately keeps the two roles apart.

**The right level of reuse:** shared low-level modules (connection, type
conversion, wire format) per backend, with separate `SourcePlugin` /
`DestinationPlugin` implementations on top. This generalizes cleanly to
non-database backends too — e.g. a future `sources/parquet` would share a
schema/type-conversion module with `destinations/parquet`, without needing a
combined read+write trait.

**Reconsider if:** the core's plugin protocol itself changes to merge the
source/destination roles into one process — unlikely, since that's the
"never wire a source's pipe directly to a destination's" rule inverted at the
plugin level.
