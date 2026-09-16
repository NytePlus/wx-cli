# Source provenance

Forked from https://github.com/pandorafuture/wx-cli at
`2abe708f55bfe135539a385df856fdc58f97fc74` (0.7.4), MIT (see LICENSE).

Fork: https://github.com/NytePlus/wx-cli, branch `codex/imcp-wechat`.
Consumed by https://github.com/NytePlus/iMCP as the `Backends/WeChat`
Git submodule. The parent repository pins an exact fork commit, not a moving
upstream branch. Commit backend changes here before updating the parent gitlink.

Local additions: `imcp-wechat` framed-stdio backend and `wx-db::incremental`.
No production command falls back to the upstream full decrypt cache or global
search scan. A failed compatibility check is surfaced to the app.

Correctness caveat: the upstream shard Timestamp bounds are inferred boundaries,
not verified minimum/maximum message timestamps. Session sort_timestamp is not
a durable change log. These must not be used as proof of lossless incremental
capture. Live readiness is deliberately gated until source invariants can be
verified. Historical import and archive querying are independent of that gate.

Manual sync (`imcp-wechat` method `sync`) replaces the disabled live path in iMCP.
It pins read snapshots, imports only rowids after each saved shard position and
commits archive/positions together. It preserves first-observed append semantics;
it does not claim to capture in-place mutations or reused rowids. Per-sync shard
discovery is metadata work; message decoding visits only newly appended rows.
