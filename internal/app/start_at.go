package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// StartAt configures the per-topic starting position used only when the
// consumer group has no committed offset for a partition. Exactly one of
// Position, Timestamp/UnixMs, or Offsets must be set.
//
// This is a safer, more expressive form of auto.offset.reset: it never
// overrides existing committed offsets, so it is idempotent across restarts.
type StartAt struct {
	// Position is one of "earliest" or "latest". Acts as a per-topic
	// override of the global auto_offset_reset for virgin partitions.
	Position string `yaml:"position,omitempty"`

	// Timestamp is an RFC3339 timestamp resolved per partition via
	// Kafka's OffsetsForTimes. Mutually exclusive with UnixMs.
	Timestamp string `yaml:"timestamp,omitempty"`

	// UnixMs is a unix epoch in milliseconds. Mutually exclusive with Timestamp.
	UnixMs *int64 `yaml:"unix_ms,omitempty"`

	// Offsets maps partition id -> starting offset. Partitions not present
	// fall back to auto_offset_reset.
	Offsets map[int32]int64 `yaml:"offsets,omitempty"`
}

// validate ensures exactly one variant is set and its values are well-formed.
func (s *StartAt) validate() error {
	if s == nil {
		return nil
	}

	hasPosition := strings.TrimSpace(s.Position) != ""
	hasTimestamp := strings.TrimSpace(s.Timestamp) != "" || s.UnixMs != nil
	hasOffsets := len(s.Offsets) > 0

	set := 0
	for _, b := range []bool{hasPosition, hasTimestamp, hasOffsets} {
		if b {
			set++
		}
	}
	if set == 0 {
		return fmt.Errorf("start_at: at least one of position, timestamp/unix_ms, or offsets must be set")
	}
	if set > 1 {
		return fmt.Errorf("start_at: exactly one of position, timestamp/unix_ms, or offsets may be set")
	}

	if hasPosition {
		switch strings.TrimSpace(s.Position) {
		case "earliest", "latest":
		default:
			return fmt.Errorf("start_at.position must be one of earliest or latest, got %q", s.Position)
		}
	}

	if hasTimestamp {
		if strings.TrimSpace(s.Timestamp) != "" && s.UnixMs != nil {
			return fmt.Errorf("start_at: timestamp and unix_ms are mutually exclusive")
		}
		if strings.TrimSpace(s.Timestamp) != "" {
			if _, err := time.Parse(time.RFC3339, strings.TrimSpace(s.Timestamp)); err != nil {
				return fmt.Errorf("start_at.timestamp must be RFC3339, got %q: %w", s.Timestamp, err)
			}
		}
		if s.UnixMs != nil && *s.UnixMs < 0 {
			return fmt.Errorf("start_at.unix_ms must be >= 0, got %d", *s.UnixMs)
		}
	}

	if hasOffsets {
		for p, o := range s.Offsets {
			if p < 0 {
				return fmt.Errorf("start_at.offsets: partition must be >= 0, got %d", p)
			}
			if o < 0 {
				return fmt.Errorf("start_at.offsets: offset for partition %d must be >= 0, got %d", p, o)
			}
		}
	}

	return nil
}

// resolvedUnixMs returns the timestamp variant in milliseconds.
// Caller must have checked that StartAt is in the timestamp variant.
func (s *StartAt) resolvedUnixMs() (int64, error) {
	if s.UnixMs != nil {
		return *s.UnixMs, nil
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(s.Timestamp))
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// startAtDecision describes how a single partition's starting position was determined.
// Used for structured logging and metrics.
type startAtDecision string

const (
	decisionCommitted        startAtDecision = "committed"
	decisionStartAtOffset    startAtDecision = "start_at_offset"
	decisionStartAtTimestamp startAtDecision = "start_at_timestamp"
	decisionStartAtPosition  startAtDecision = "start_at_position"
	decisionFallbackReset    startAtDecision = "fallback_auto_reset"
)

// resolveAssignment computes the starting offset for each newly assigned partition.
// It honours committed offsets first (never overrides), and applies StartAt only to
// virgin partitions. Returns the partitions ready to pass to consumer.Assign and
// per-partition decisions for logging/metrics.
//
// The consumer is used to query Committed and OffsetsForTimes; pass timeoutMs for both.
func resolveAssignment(
	c committedQueryer,
	parts []kafka.TopicPartition,
	sa *StartAt,
	timeoutMs int,
) ([]kafka.TopicPartition, []partitionDecision, error) {
	decisions := make([]partitionDecision, len(parts))

	// Default: pass through unchanged using OffsetStored so librdkafka uses
	// the committed offset (or auto.offset.reset on miss).
	out := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		p.Offset = kafka.OffsetStored
		out[i] = p
		decisions[i] = partitionDecision{Partition: parts[i].Partition, Decision: decisionCommitted}
	}

	if sa == nil {
		return out, decisions, nil
	}

	// Query committed offsets to learn which partitions are virgin.
	query := make([]kafka.TopicPartition, len(parts))
	copy(query, parts)
	for i := range query {
		query[i].Offset = kafka.OffsetInvalid
	}
	committed, err := c.Committed(query, timeoutMs)
	if err != nil {
		return nil, nil, fmt.Errorf("start_at: failed to query committed offsets: %w", err)
	}

	// Build a lookup by partition id.
	committedByPart := make(map[int32]kafka.Offset, len(committed))
	for _, p := range committed {
		committedByPart[p.Partition] = p.Offset
	}

	// Pre-compute starting offsets for each variant.
	switch {
	case strings.TrimSpace(sa.Position) != "":
		var startOffset kafka.Offset
		if strings.TrimSpace(sa.Position) == "earliest" {
			startOffset = kafka.OffsetBeginning
		} else {
			startOffset = kafka.OffsetEnd
		}
		for i, p := range parts {
			if isCommitted(committedByPart[p.Partition]) {
				continue
			}
			out[i].Offset = startOffset
			decisions[i] = partitionDecision{Partition: p.Partition, Decision: decisionStartAtPosition, Offset: int64(startOffset)}
		}

	case len(sa.Offsets) > 0:
		for i, p := range parts {
			if isCommitted(committedByPart[p.Partition]) {
				continue
			}
			off, ok := sa.Offsets[p.Partition]
			if !ok {
				decisions[i] = partitionDecision{Partition: p.Partition, Decision: decisionFallbackReset}
				// Leave OffsetStored — librdkafka will apply auto.offset.reset.
				continue
			}
			out[i].Offset = kafka.Offset(off)
			decisions[i] = partitionDecision{Partition: p.Partition, Decision: decisionStartAtOffset, Offset: off}
		}

	default: // timestamp variant
		ms, err := sa.resolvedUnixMs()
		if err != nil {
			return nil, nil, fmt.Errorf("start_at: invalid timestamp: %w", err)
		}

		// Build a query of virgin partitions only, with .Offset set to the timestamp.
		var virginIdx []int
		var tsQuery []kafka.TopicPartition
		for i, p := range parts {
			if isCommitted(committedByPart[p.Partition]) {
				continue
			}
			virginIdx = append(virginIdx, i)
			q := p
			q.Offset = kafka.Offset(ms)
			tsQuery = append(tsQuery, q)
		}
		if len(tsQuery) == 0 {
			return out, decisions, nil
		}

		resolved, err := c.OffsetsForTimes(tsQuery, timeoutMs)
		if err != nil {
			return nil, nil, fmt.Errorf("start_at: OffsetsForTimes failed: %w", err)
		}
		// Map back by partition.
		resolvedByPart := make(map[int32]kafka.Offset, len(resolved))
		for _, p := range resolved {
			resolvedByPart[p.Partition] = p.Offset
		}
		for _, i := range virginIdx {
			p := parts[i]
			off, ok := resolvedByPart[p.Partition]
			if !ok || off < 0 {
				// No message at-or-after the timestamp: start at end (skip historical data).
				out[i].Offset = kafka.OffsetEnd
				decisions[i] = partitionDecision{Partition: p.Partition, Decision: decisionStartAtTimestamp, Offset: int64(kafka.OffsetEnd)}
				continue
			}
			out[i].Offset = off
			decisions[i] = partitionDecision{Partition: p.Partition, Decision: decisionStartAtTimestamp, Offset: int64(off)}
		}
	}

	return out, decisions, nil
}

// committedQueryer is the subset of *kafka.Consumer used by resolveAssignment;
// abstracted for testability.
type committedQueryer interface {
	Committed(partitions []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error)
	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error)
}

// partitionDecision records the per-partition starting position decision for
// structured logging and metric emission.
type partitionDecision struct {
	Partition int32
	Decision  startAtDecision
	Offset    int64 // populated when Decision != decisionCommitted
}

// isCommitted reports whether a Committed() response indicates a real prior commit.
// Uncommitted partitions return OffsetInvalid (-1001).
func isCommitted(o kafka.Offset) bool {
	return int64(o) >= 0
}
