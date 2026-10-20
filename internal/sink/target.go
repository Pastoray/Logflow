package sink

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileTargetPrefix marks a sink-group target as a local directory (rolling
// files, same as a `file` sink) instead of a gRPC address: e.g.
// "file:/var/spool/logflow". gRPC host:port addresses never start with it.
const FileTargetPrefix = "file:"

// IsFileTarget reports whether a sink-group target is a file backend.
func IsFileTarget(target string) bool { return strings.HasPrefix(target, FileTargetPrefix) }

// FileTargetDir returns the directory of a file target.
func FileTargetDir(target string) string { return strings.TrimPrefix(target, FileTargetPrefix) }

// fileTargetHealthy is the file backend's equivalent of a gRPC Ping: can
// we actually create (and remove) a file in the directory right now?
func fileTargetHealthy(dir string) bool {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false
	}
	f, err := os.CreateTemp(dir, ".logflow-probe-*")
	if err != nil {
		return false
	}
	name := f.Name()
	_, werr := f.Write([]byte("probe"))
	cerr := f.Close()
	rerr := os.Remove(filepath.Clean(name))
	return werr == nil && cerr == nil && rerr == nil
}

// ValidateTarget checks a sink-group target string.
func ValidateTarget(target string) error {
	if target == "" {
		return fmt.Errorf("empty target")
	}
	if IsFileTarget(target) && FileTargetDir(target) == "" {
		return fmt.Errorf("file target %q has no directory", target)
	}
	return nil
}
