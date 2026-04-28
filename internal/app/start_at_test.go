package app

import (
	"errors"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestStartAtValidate(t *testing.T) {
	ms := int64(1700000000000)
	negMs := int64(-1)

	tests := []struct {
		name    string
		sa      *StartAt
		wantErr string
	}{
		{name: "nil is ok", sa: nil},
		{name: "empty rejected", sa: &StartAt{}, wantErr: "at least one"},
		{name: "position earliest", sa: &StartAt{Position: "earliest"}},
		{name: "position latest", sa: &StartAt{Position: "latest"}},
		{name: "position invalid", sa: &StartAt{Position: "beginning"}, wantErr: "position must be one of"},
		{name: "timestamp rfc3339", sa: &StartAt{Timestamp: "2026-04-01T00:00:00Z"}},
		{name: "timestamp invalid", sa: &StartAt{Timestamp: "yesterday"}, wantErr: "RFC3339"},
		{name: "unix_ms ok", sa: &StartAt{UnixMs: &ms}},
		{name: "unix_ms negative", sa: &StartAt{UnixMs: &negMs}, wantErr: "unix_ms must be >= 0"},
		{name: "ts and unix_ms exclusive", sa: &StartAt{Timestamp: "2026-04-01T00:00:00Z", UnixMs: &ms}, wantErr: "mutually exclusive"},
		{name: "offsets ok", sa: &StartAt{Offsets: map[int32]int64{0: 10, 1: 20}}},
		{name: "offsets negative offset", sa: &StartAt{Offsets: map[int32]int64{0: -5}}, wantErr: "offset for partition 0"},
		{name: "two variants set", sa: &StartAt{Position: "earliest", Offsets: map[int32]int64{0: 1}}, wantErr: "exactly one of"},
		{name: "position and timestamp", sa: &StartAt{Position: "latest", Timestamp: "2026-04-01T00:00:00Z"}, wantErr: "exactly one of"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sa.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected ok, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestValidateConfigStartAt(t *testing.T) {
	cfg := validConfig()
	cfg.TopicTables[0].StartAt = &StartAt{Position: "wrong"}
	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "topic_tables[0]") || !strings.Contains(err.Error(), "position") {
		t.Fatalf("expected per-topic start_at validation error, got %v", err)
	}
}

// fakeQueryer implements committedQueryer for unit tests.
type fakeQueryer struct {
	committed      []kafka.TopicPartition
	committedErr   error
	resolvedTimes  []kafka.TopicPartition
	timesErr       error
	committedCalls int
	timesCalls     int
	timesQueryRecv []kafka.TopicPartition
}

func (f *fakeQueryer) Committed(parts []kafka.TopicPartition, _ int) ([]kafka.TopicPartition, error) {
	f.committedCalls++
	if f.committedErr != nil {
		return nil, f.committedErr
	}
	if f.committed != nil {
		return f.committed, nil
	}
	// Default: nothing committed.
	out := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		p.Offset = kafka.OffsetInvalid
		out[i] = p
	}
	return out, nil
}

func (f *fakeQueryer) OffsetsForTimes(times []kafka.TopicPartition, _ int) ([]kafka.TopicPartition, error) {
	f.timesCalls++
	f.timesQueryRecv = times
	if f.timesErr != nil {
		return nil, f.timesErr
	}
	return f.resolvedTimes, nil
}

func partsFor(topic string, partitions ...int32) []kafka.TopicPartition {
	t := topic
	out := make([]kafka.TopicPartition, len(partitions))
	for i, p := range partitions {
		out[i] = kafka.TopicPartition{Topic: &t, Partition: p, Offset: kafka.OffsetInvalid}
	}
	return out
}

func TestResolveAssignment_NilStartAt_PassesThrough(t *testing.T) {
	q := &fakeQueryer{}
	parts := partsFor("t", 0, 1)
	out, decisions, err := resolveAssignment(q, parts, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if q.committedCalls != 0 {
		t.Fatalf("Committed should not be called when StartAt is nil")
	}
	for _, p := range out {
		if p.Offset != kafka.OffsetStored {
			t.Fatalf("expected OffsetStored, got %v", p.Offset)
		}
	}
	for _, d := range decisions {
		if d.Decision != decisionCommitted {
			t.Fatalf("expected committed decision, got %v", d.Decision)
		}
	}
}

func TestResolveAssignment_PositionEarliest_AppliesToVirgin(t *testing.T) {
	topic := "t"
	q := &fakeQueryer{
		committed: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: 500},                 // committed
			{Topic: &topic, Partition: 1, Offset: kafka.OffsetInvalid}, // virgin
		},
	}
	parts := partsFor("t", 0, 1)
	out, decisions, err := resolveAssignment(q, parts, &StartAt{Position: "earliest"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Offset != kafka.OffsetStored {
		t.Fatalf("p0 already committed: expected OffsetStored, got %v", out[0].Offset)
	}
	if out[1].Offset != kafka.OffsetBeginning {
		t.Fatalf("p1 virgin: expected OffsetBeginning, got %v", out[1].Offset)
	}
	if decisions[0].Decision != decisionCommitted {
		t.Fatalf("p0: expected committed decision, got %v", decisions[0].Decision)
	}
	if decisions[1].Decision != decisionStartAtPosition {
		t.Fatalf("p1: expected start_at_position decision, got %v", decisions[1].Decision)
	}
}

func TestResolveAssignment_PositionLatest_AppliesToVirgin(t *testing.T) {
	q := &fakeQueryer{} // all virgin
	parts := partsFor("t", 0)
	out, _, err := resolveAssignment(q, parts, &StartAt{Position: "latest"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Offset != kafka.OffsetEnd {
		t.Fatalf("expected OffsetEnd, got %v", out[0].Offset)
	}
}

func TestResolveAssignment_OffsetsMap(t *testing.T) {
	q := &fakeQueryer{} // all virgin
	parts := partsFor("t", 0, 1, 2)
	sa := &StartAt{Offsets: map[int32]int64{0: 100, 2: 300}}
	out, decisions, err := resolveAssignment(q, parts, sa, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Offset != 100 {
		t.Fatalf("p0: expected 100, got %v", out[0].Offset)
	}
	if out[1].Offset != kafka.OffsetStored {
		t.Fatalf("p1 not in map: expected OffsetStored (fallback), got %v", out[1].Offset)
	}
	if decisions[1].Decision != decisionFallbackReset {
		t.Fatalf("p1: expected fallback_auto_reset, got %v", decisions[1].Decision)
	}
	if out[2].Offset != 300 {
		t.Fatalf("p2: expected 300, got %v", out[2].Offset)
	}
}

func TestResolveAssignment_OffsetsMap_HonoursCommitted(t *testing.T) {
	topic := "t"
	q := &fakeQueryer{
		committed: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: 999}, // already committed, must not be overridden
		},
	}
	parts := partsFor("t", 0)
	sa := &StartAt{Offsets: map[int32]int64{0: 100}}
	out, decisions, err := resolveAssignment(q, parts, sa, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Offset != kafka.OffsetStored {
		t.Fatalf("expected OffsetStored (committed wins), got %v", out[0].Offset)
	}
	if decisions[0].Decision != decisionCommitted {
		t.Fatalf("expected committed decision, got %v", decisions[0].Decision)
	}
}

func TestResolveAssignment_Timestamp_Resolves(t *testing.T) {
	topic := "t"
	q := &fakeQueryer{
		// Both virgin (default committed = OffsetInvalid).
		resolvedTimes: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: 42},
			{Topic: &topic, Partition: 1, Offset: -1}, // no message at-or-after timestamp
		},
	}
	parts := partsFor("t", 0, 1)
	sa := &StartAt{Timestamp: "2026-04-01T00:00:00Z"}
	out, decisions, err := resolveAssignment(q, parts, sa, 100)
	if err != nil {
		t.Fatal(err)
	}
	if q.timesCalls != 1 {
		t.Fatalf("expected 1 OffsetsForTimes call, got %d", q.timesCalls)
	}
	if out[0].Offset != 42 {
		t.Fatalf("p0: expected 42, got %v", out[0].Offset)
	}
	if out[1].Offset != kafka.OffsetEnd {
		t.Fatalf("p1: expected OffsetEnd (no-msg fallback), got %v", out[1].Offset)
	}
	if decisions[0].Decision != decisionStartAtTimestamp {
		t.Fatalf("p0 decision: %v", decisions[0].Decision)
	}
}

func TestResolveAssignment_Timestamp_SkipsCommittedFromQuery(t *testing.T) {
	topic := "t"
	q := &fakeQueryer{
		committed: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: 500}, // committed
			{Topic: &topic, Partition: 1, Offset: kafka.OffsetInvalid},
		},
		resolvedTimes: []kafka.TopicPartition{
			{Topic: &topic, Partition: 1, Offset: 77},
		},
	}
	parts := partsFor("t", 0, 1)
	out, _, err := resolveAssignment(q, parts, &StartAt{Timestamp: "2026-04-01T00:00:00Z"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.timesQueryRecv) != 1 || q.timesQueryRecv[0].Partition != 1 {
		t.Fatalf("OffsetsForTimes should only be queried for virgin partitions, got %+v", q.timesQueryRecv)
	}
	if out[0].Offset != kafka.OffsetStored {
		t.Fatalf("p0 committed: expected OffsetStored, got %v", out[0].Offset)
	}
	if out[1].Offset != 77 {
		t.Fatalf("p1: expected 77, got %v", out[1].Offset)
	}
}

func TestResolveAssignment_CommittedQueryError(t *testing.T) {
	q := &fakeQueryer{committedErr: errors.New("kafka down")}
	parts := partsFor("t", 0)
	_, _, err := resolveAssignment(q, parts, &StartAt{Position: "earliest"}, 100)
	if err == nil || !strings.Contains(err.Error(), "kafka down") {
		t.Fatalf("expected propagated error, got %v", err)
	}
}

func TestStartAtResolvedUnixMs(t *testing.T) {
	sa := &StartAt{Timestamp: "2026-04-01T00:00:00Z"}
	got, err := sa.resolvedUnixMs()
	if err != nil {
		t.Fatal(err)
	}
	if got != 1775001600000 {
		t.Fatalf("expected 1775001600000, got %d", got)
	}

	ms := int64(123)
	sa2 := &StartAt{UnixMs: &ms}
	got2, err := sa2.resolvedUnixMs()
	if err != nil || got2 != 123 {
		t.Fatalf("expected 123, got %d (%v)", got2, err)
	}
}
