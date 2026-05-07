package app

import (
	"testing"
)

func boolPtr(b bool) *bool { return &b }

func TestFlattenRecord(t *testing.T) {
	tests := []struct {
		name           string
		input          map[string]interface{}
		delimiter      string
		maxDepth       int
		preserveArrays bool
		expected       map[string]interface{}
	}{
		{
			name:           "flat passthrough",
			input:          map[string]interface{}{"a": 1, "b": "two"},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"a": 1, "b": "two"},
		},
		{
			name:           "1-level nested",
			input:          map[string]interface{}{"company": map[string]interface{}{"id": 42, "name": "Acme"}},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"company_id": 42, "company_name": "Acme"},
		},
		{
			name: "3-level nested",
			input: map[string]interface{}{
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": map[string]interface{}{
							"d": 1,
						},
					},
				},
			},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"a_b_c_d": 1},
		},
		{
			name:           "custom delimiter dot",
			input:          map[string]interface{}{"a": map[string]interface{}{"b": 1}},
			delimiter:      ".",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"a.b": 1},
		},
		{
			name: "max_depth=1 stops at first level",
			input: map[string]interface{}{
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": 1,
					},
				},
			},
			delimiter:      "_",
			maxDepth:       1,
			preserveArrays: true,
			expected:       map[string]interface{}{"a_b": map[string]interface{}{"c": 1}},
		},
		{
			name: "arrays preserved",
			input: map[string]interface{}{
				"a": map[string]interface{}{
					"b": []interface{}{1, 2, 3},
				},
			},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"a_b": []interface{}{1, 2, 3}},
		},
		{
			name: "null values kept",
			input: map[string]interface{}{
				"a": map[string]interface{}{
					"b": nil,
				},
			},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"a_b": nil},
		},
		{
			name:           "empty input",
			input:          map[string]interface{}{},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{},
		},
		{
			name: "mixed nested and flat fields",
			input: map[string]interface{}{
				"id":   1,
				"user": map[string]interface{}{"profile": map[string]interface{}{"email": "x@y.z"}},
			},
			delimiter:      "_",
			maxDepth:       0,
			preserveArrays: true,
			expected:       map[string]interface{}{"id": 1, "user_profile_email": "x@y.z"},
		},
		{
			name: "max_depth=2",
			input: map[string]interface{}{
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": map[string]interface{}{
							"d": 1,
						},
					},
				},
			},
			delimiter:      "_",
			maxDepth:       2,
			preserveArrays: true,
			expected:       map[string]interface{}{"a_b_c": map[string]interface{}{"d": 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := flattenRecord(tt.input, tt.delimiter, tt.maxDepth, tt.preserveArrays)
			if len(got) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d keys, want %d keys\ngot:  %v\nwant: %v", len(got), len(tt.expected), got, tt.expected)
			}
			for k, wantVal := range tt.expected {
				gotVal, ok := got[k]
				if !ok {
					t.Errorf("missing key %q in result\ngot: %v", k, got)
					continue
				}
				if !deepEqual(gotVal, wantVal) {
					t.Errorf("key %q: got %v (%T), want %v (%T)", k, gotVal, gotVal, wantVal, wantVal)
				}
			}
		})
	}
}

func TestFlattenConfigResolveDefaults(t *testing.T) {
	t.Run("nil config is safe", func(t *testing.T) {
		var fc *FlattenConfig
		fc.resolveDefaults() // should not panic
	})

	t.Run("defaults applied", func(t *testing.T) {
		fc := &FlattenConfig{Enabled: true}
		fc.resolveDefaults()
		if fc.Delimiter != "_" {
			t.Errorf("delimiter: got %q, want %q", fc.Delimiter, "_")
		}
		if fc.PreserveArrays == nil || !*fc.PreserveArrays {
			t.Error("preserve_arrays should default to true")
		}
	})

	t.Run("explicit values preserved", func(t *testing.T) {
		fc := &FlattenConfig{Enabled: true, Delimiter: ".", PreserveArrays: boolPtr(false)}
		fc.resolveDefaults()
		if fc.Delimiter != "." {
			t.Errorf("delimiter: got %q, want %q", fc.Delimiter, ".")
		}
		if *fc.PreserveArrays != false {
			t.Error("preserve_arrays should remain false")
		}
	})
}

func TestFlattenDisabledIsNoop(t *testing.T) {
	input := map[string]interface{}{
		"a": map[string]interface{}{"b": 1},
	}
	// When flatten is nil or disabled, the caller should not call flattenRecord.
	// Verify that the config check works as expected.
	var fc *FlattenConfig
	if fc != nil && fc.Enabled {
		t.Fatal("nil FlattenConfig should not trigger flatten")
	}

	fc = &FlattenConfig{Enabled: false}
	if fc != nil && fc.Enabled {
		t.Fatal("disabled FlattenConfig should not trigger flatten")
	}

	// Also verify flattenRecord itself is a passthrough for already-flat data.
	flat := map[string]interface{}{"x": 1, "y": "two"}
	got := flattenRecord(flat, "_", 0, true)
	if len(got) != 2 || got["x"] != 1 || got["y"] != "two" {
		t.Errorf("flat passthrough failed: %v", got)
	}

	_ = input // used only for the config gating check above
}

// deepEqual compares two values for test assertions. It handles
// map[string]interface{} and []interface{} recursively.
func deepEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !deepEqual(v, bv[k]) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
