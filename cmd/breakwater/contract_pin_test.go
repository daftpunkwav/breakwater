/**
 * @file contract_pin_test
 * @description The composition-root contract pins: every format the
 * process serves must resolve to the wire that names itself.
 *
 * Adding a Format member means extending every switch over it
 * (protocol.Ingest, protocol.WireFor, the route table in
 * internal/server) — until then the new format silently behaves as
 * openai-chat. The single formats slice at the composition root is the
 * checklist; this test walks it, so a missed WireFor case fails here
 * instead of answering real traffic on the wrong wire.
 */
package main

import (
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func TestServedFormatsResolveToTheirOwnWire(t *testing.T) {
	t.Parallel()
	if len(formats) == 0 {
		t.Fatal("no formats served: the composition root must register every client surface")
	}
	for _, format := range formats {
		if got := protocol.WireFor(format).Format(); got != format {
			t.Errorf("WireFor(%s) = %s, want the wire that names itself — extend protocol.WireFor's switch for the new format", format, got)
		}
	}
}
