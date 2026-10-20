package storage

import "errors"

// ErrInvalidArgument distinguishes rejected client input from storage failures.
var ErrInvalidArgument = errors.New("invalid argument")
