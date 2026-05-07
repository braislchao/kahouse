package app

// flattenRecord recursively walks a map[string]interface{} and collapses
// nested maps by joining keys with delimiter, matching the behavior of
// Kafka Connect's org.apache.kafka.connect.transforms.Flatten$Value SMT.
//
// Arrays are preserved as-is (not descended into) when preserveArrays is true,
// matching Kafka Connect's default behavior. maxDepth=0 means unlimited
// recursion; a positive value caps the recursion depth.
func flattenRecord(record map[string]interface{}, delimiter string, maxDepth int, preserveArrays bool) map[string]interface{} {
	out := make(map[string]interface{}, len(record))
	flattenRecurse(record, "", delimiter, maxDepth, 0, preserveArrays, out)
	return out
}

func flattenRecurse(m map[string]interface{}, prefix, delimiter string, maxDepth, currentDepth int, preserveArrays bool, out map[string]interface{}) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + delimiter + k
		}
		switch val := v.(type) {
		case map[string]interface{}:
			if maxDepth > 0 && currentDepth >= maxDepth {
				out[key] = val // stop recursion at max depth
			} else {
				flattenRecurse(val, key, delimiter, maxDepth, currentDepth+1, preserveArrays, out)
			}
		default:
			// Arrays, scalars, and nil all pass through unchanged.
			out[key] = v
		}
	}
}
