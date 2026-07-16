package storage

import (
	"fmt"
	"testing"
)

func TestIndexShardHexMatchesLegacyFormatting(t *testing.T) {
	for value := 0; value < len(indexShardHex); value++ {
		if got, want := indexShardHex[value], fmt.Sprintf("%02x", value); got != want {
			t.Fatalf("indexShardHex[%d] = %q, want %q", value, got, want)
		}
	}
}
