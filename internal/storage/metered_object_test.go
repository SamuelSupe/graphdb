package storage

import (
	"fmt"
	"testing"
)

func TestObjectOperationStatusClassifiesUnsupportedConditionalDelete(t *testing.T) {
	err := fmt.Errorf("%w: %w", ErrConflict, ErrConditionalDeleteUnsupported)
	if got := objectOperationStatus(err); got != "unsupported" {
		t.Fatalf("status = %q, want unsupported", got)
	}
}
