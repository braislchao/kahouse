# Per-topic starting offset / timestamp

Status: Implemented
Owner: kahouse maintainers

## 1. Motivation

Today kahouse exposes only a single global `auto_offset_reset` (`earliest` / `latest` / `none`). Operators who want to start a new pipeline from a known point in time, or replay a single topic from a known offset after a downstream incident, must use external tooling such as `kafka-consumer-groups.sh --reset-offsets` before launching the process. That workflow is out-of-band, error-prone, and leaves no audit trail in the kahouse config.

This spec adds a per-topic `start_at` field that declares an initial starting position. It is applied **only when the consumer group has no committed offset for a partition** -- so it is fully idempotent across restarts and never overrides committed progress.

## 2. User-facing config

Per-topic, optional `start_at` field. Tagged union: exactly one variant must be set.

```yaml
topic_tables:
  # A. Named position (per-topic override of auto_offset_reset)
  - topic: events.foo
    table: events_foo
    start_at:
      position: earliest    # earliest | latest

  # B. Timestamp (resolved per partition via OffsetsForTimes)
  - topic: events.bar
    table: events_bar
    start_at:
      timestamp: "2026-04-01T00:00:00Z"  # RFC3339
      # or:
      # unix_ms: 1717200000000

  # C. Explicit per-partition offsets
  - topic: events.baz
    table: events_baz
    start_at:
      offsets:
        0: 12345
        1: 9000
        2: 0
```

Semantics:

- Applied only on first assignment of a partition that has no committed offset for the group `kahouse-<group_id>-<topic>`. Once committed, kahouse never re-seeks.
- Mixed state: if partitions 0,1 already committed but 2 is new, only partition 2 is seeked.
- Variant C: missing partitions fall back to the global `auto_offset_reset`. A structured log line and a `fallback_auto_reset` metric are emitted.
- Variant B: partitions with no message at-or-after the timestamp start at the partition end (`OffsetEnd`).

## 3. Go types

`internal/app/start_at.go`:

```go
type StartAt struct {
    Position  string          `yaml:"position,omitempty"`
    Timestamp string          `yaml:"timestamp,omitempty"`
    UnixMs    *int64          `yaml:"unix_ms,omitempty"`
    Offsets   map[int32]int64 `yaml:"offsets,omitempty"`
}
```

`internal/app/config.go` adds an optional `StartAt *StartAt` field to `TopicTableMapping`.

## 4. Validation

In `validateConfig`, for each mapping with `StartAt != nil`:

- Exactly one of `{Position, Timestamp|UnixMs, Offsets}` must be set.
- `Position` ∈ {`earliest`, `latest`}.
- `Timestamp` parseable as RFC3339; `UnixMs >= 0`; mutually exclusive.
- `Offsets`: non-empty, partition keys ≥ 0, offset values ≥ 0.

Errors are prefixed with `topic_tables[i]:` for actionable diagnostics.

## 5. Implementation

`internal/app/sink.go` switches from `consumer.SubscribeTopics([...], nil)` to `consumer.Subscribe(topic, t.onRebalance)`. The rebalance callback handles `AssignedPartitions` / `RevokedPartitions`:

1. On `AssignedPartitions`, call `resolveAssignment` (in `start_at.go`).
2. `resolveAssignment` queries `consumer.Committed(parts, 10s)`. For each partition:
   - If committed (`offset >= 0`) → set `OffsetStored` (no seek).
   - Else apply the configured `StartAt` variant:
     - `position: earliest|latest` → `OffsetBeginning|OffsetEnd`.
     - `offsets` → use the configured offset; missing keys fall back to `OffsetStored` so librdkafka applies `auto.offset.reset`.
     - `timestamp`/`unix_ms` → resolve via `consumer.OffsetsForTimes` (only for virgin partitions); if no message at-or-after, use `OffsetEnd`.
3. Call `consumer.Assign(resolved)`.
4. On `RevokedPartitions` → `consumer.Unassign()`.

If `Committed` or `OffsetsForTimes` fails, kahouse logs the error and falls back to a plain `Assign` with `OffsetStored` so processing can proceed; the next rebalance will retry.

## 6. Metrics & logging

- New counter `kahouse_start_at_applied_total{topic,decision}` incremented per partition on assignment. `decision` is one of: `committed`, `start_at_offset`, `start_at_timestamp`, `start_at_position`, `fallback_auto_reset`.
- Per-partition `INFO` log line on first apply (skipped for the `committed` decision to avoid noise).
- `WARN` on Committed/OffsetsForTimes failure with fallback explanation.

## 7. Testing

Unit tests in `internal/app/start_at_test.go`:

- `TestStartAtValidate`: every accept/reject case for the validator.
- `TestValidateConfigStartAt`: per-topic error wrapping.
- `TestResolveAssignment_*`:
  - nil StartAt passes through with `OffsetStored`.
  - position earliest/latest applies only to virgin partitions.
  - offsets map: virgin partitions seek; partitions with committed offsets are untouched; partitions missing from the map fall back.
  - timestamp variant: only virgin partitions are sent to `OffsetsForTimes`; partitions with no resolved message default to `OffsetEnd`.
  - propagation of `Committed` errors.

The resolver is fronted by a small `committedQueryer` interface so unit tests do not need a live Kafka cluster.

## 8. Documentation

- `docs/configuration.md`: new "Per-topic starting position (`start_at`)" subsection plus validation rules.
- `config.yaml.example`: commented example covering all three variants.

## 9. Backward compatibility

- `start_at` is optional. Omitting it preserves today's behavior exactly.
- `auto_offset_reset` remains the global fallback for partitions without `start_at` or with the `offsets` variant missing a partition.
- Consumer group naming (`kahouse-<group_id>-<topic>`) is unchanged.

## 10. Out of scope

- Forced re-seek on every restart (rejected: footgun, encourages duplicates).
- CLI subcommand to externally reset offsets (operators can keep using `kafka-consumer-groups.sh`).
- Per-partition timestamps (use a single `timestamp`; per-partition precision is covered by `offsets`).
- HTTP/admin API to change starting positions at runtime.

## 11. Rollout

Single PR. The change is opt-in: existing configs run unchanged because `start_at` defaults to nil.
