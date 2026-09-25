/**
 * @file main_test
 * @description The process entry point, exercised as a subprocess: a
 * broken configuration must turn into a non-zero exit code.
 */
package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainExitsNonZeroOnBadConfiguration(t *testing.T) {
	if os.Getenv("BE_MAIN") == "1" {
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMainExitsNonZeroOnBadConfiguration")
	cmd.Env = append(os.Environ(),
		"BE_MAIN=1",
		"BREAKWATER_STREAM_TIMEOUT=-1s", // rejected by config.Load
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("bad configuration must produce a non-zero exit")
	}
	if !strings.Contains(string(out), "gateway terminated") {
		t.Fatalf("output = %s, want the termination log line", out)
	}
}
