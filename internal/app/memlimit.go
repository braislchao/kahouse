package app

import (
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// applyMemoryLimit sets Go's soft memory limit (GOMEMLIMIT equivalent) to ~90%
// of the container's cgroup memory limit, so the garbage collector works harder
// as it approaches the limit instead of letting RSS grow until the kernel
// OOMKills the pod. Without this, the Go runtime is unaware of the cgroup limit.
//
// An explicit GOMEMLIMIT env var (applied by the runtime at startup) always
// wins; we only fill in a limit when none is set. If there is no finite cgroup
// limit (e.g. local runs) the Go default is left untouched.
func applyMemoryLimit(sugar *zap.SugaredLogger) {
	if cur := debug.SetMemoryLimit(-1); cur != math.MaxInt64 {
		sugar.Infow("Soft memory limit already set (GOMEMLIMIT), not overriding", "soft_limit_bytes", cur)
		return
	}
	limit, ok := cgroupMemoryLimit()
	if !ok {
		return
	}
	soft := limit / 10 * 9 // 90%, integer math to avoid float rounding
	debug.SetMemoryLimit(soft)
	sugar.Infow("Applied soft memory limit from cgroup", "cgroup_limit_bytes", limit, "soft_limit_bytes", soft)
}

// cgroupMemoryLimit returns the container memory limit in bytes, reading cgroup
// v2 first then falling back to v1. Returns false if there is no finite limit.
func cgroupMemoryLimit() (int64, bool) {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil { // cgroup v2
		return parseMemMax(string(b))
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil { // cgroup v1
		return parseMemMax(string(b))
	}
	return 0, false
}

// parseMemMax parses a cgroup memory-limit file value. "max" (v2) and the huge
// sentinel v1 uses for "unlimited" both mean no finite limit.
func parseMemMax(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 || v >= (1<<62) { // v1 unlimited sentinel is ~9.2e18
		return 0, false
	}
	return v, true
}
