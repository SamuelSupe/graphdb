package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

type config struct {
	writer  string
	reader  string
	tenant  string
	timeout time.Duration
}

func main() {
	cfg := parseConfig()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	runner := &runner{
		cfg:    cfg,
		writer: newClient(cfg.writer, cfg.tenant),
		reader: newClient(cfg.reader, cfg.tenant),
		other:  newClient(cfg.reader, cfg.tenant+"-other"),
	}
	if err := runner.run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS e2e tenant=%s writer=%s reader=%s\n", cfg.tenant, cfg.writer, cfg.reader)
}

func parseConfig() config {
	cfg := config{}
	flag.StringVar(&cfg.writer, "writer", "http://127.0.0.1:38080", "writer base URL")
	flag.StringVar(&cfg.reader, "reader", "http://127.0.0.1:38081", "reader base URL")
	flag.StringVar(&cfg.tenant, "tenant", "", "tenant id; generated when empty")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "overall timeout")
	flag.Parse()
	if cfg.tenant == "" {
		cfg.tenant = fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	}
	return cfg
}
