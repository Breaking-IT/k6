package tests

import "testing"

func requireFullBuild(t testing.TB) {
	t.Helper()
	if leanBuild {
		t.Skip("requires cloud, REST API, or an output excluded from the lean build")
	}
}
