package main

import (
	"fmt"
	"os"
)

func main() {
	cfg := parseConfig()
	if cfg.input == "" {
		fmt.Fprintln(os.Stderr, "-in is required")
		os.Exit(2)
	}
	file, err := os.Open(cfg.input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input: %v\n", err)
		os.Exit(2)
	}
	defer file.Close()

	report := newReport(cfg)
	if err := readEvents(file, func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		fmt.Fprintf(os.Stderr, "read events: %v\n", err)
		os.Exit(2)
	}
	report.print(os.Stdout)
	if violations := report.violations(); len(violations) > 0 {
		for _, violation := range violations {
			fmt.Fprintf(os.Stderr, "violation: %s\n", violation)
		}
		os.Exit(1)
	}
}
