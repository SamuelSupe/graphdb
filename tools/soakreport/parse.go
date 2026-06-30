package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type event struct {
	Kind string
	At   time.Time
	Raw  map[string]any
}

func readEvents(r io.Reader, fn func(event) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var raw map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		kind, _ := raw["event"].(string)
		if kind == "" {
			return fmt.Errorf("line %d: missing event", line)
		}
		var at time.Time
		if value, _ := raw["ts"].(string); value != "" {
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return fmt.Errorf("line %d: parse ts: %w", line, err)
			}
			at = parsed
		}
		if err := fn(event{Kind: kind, At: at, Raw: raw}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func floatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}
