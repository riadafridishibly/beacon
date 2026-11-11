package main

import (
	"runtime/debug"

	"golang.org/x/mod/module"
)

// version names the build: its release tag, or for an untagged build the
// commit it was built from. go build records both; go run records neither.
func version(bi *debug.BuildInfo) string {
	v := bi.Main.Version
	if v != "" && v != "(devel)" && !module.IsPseudoVersion(v) {
		return v
	}
	var rev string
	var dirty bool
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "devel"
	}
	rev = rev[:min(7, len(rev))]
	if dirty {
		rev += "+dirty"
	}
	return rev
}
