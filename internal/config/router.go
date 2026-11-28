package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

type RouterConfig struct {
	Addr               string
	Token              string
	AuthToken          string
	AdminToken         string
	RequireTenantScope bool
	Catalog            sharding.Shard
	GraphLimits        sharding.GraphAdmissionLimits
}

func LoadRouter() (RouterConfig, error) {
	cfg := RouterConfig{
		Addr:       getenv("GRAPHDB_ADDR", ":8080"),
		Token:      os.Getenv("GRAPHDB_ROUTER_TOKEN"),
		AuthToken:  strings.TrimSpace(os.Getenv("GRAPHDB_AUTH_TOKEN")),
		AdminToken: strings.TrimSpace(os.Getenv("GRAPHDB_ADMIN_TOKEN")),
		Catalog:    sharding.Shard{ID: "catalog", ClusterID: os.Getenv("GRAPHDB_ROUTER_CATALOG_CLUSTER_ID")},
	}
	if err := loadBoolEnv("GRAPHDB_REQUIRE_TENANT_SCOPE", &cfg.RequireTenantScope); err != nil {
		return cfg, err
	}
	if len(cfg.Token) < 32 {
		return cfg, fmt.Errorf("GRAPHDB_ROUTER_TOKEN must contain at least 32 bytes")
	}
	if err := json.Unmarshal([]byte(os.Getenv("GRAPHDB_ROUTER_CATALOG_PEERS")), &cfg.Catalog.Peers); err != nil {
		return cfg, fmt.Errorf("GRAPHDB_ROUTER_CATALOG_PEERS must map node IDs to private HTTP origins: %w", err)
	}
	cfg.GraphLimits = sharding.DefaultGraphAdmissionLimits()
	if err := loadIntEnv("GRAPHDB_ROUTER_GRAPH_READS_PER_SHARD", &cfg.GraphLimits.Reads); err != nil {
		return cfg, err
	}
	if err := loadIntEnv("GRAPHDB_ROUTER_GRAPH_WRITES_PER_SHARD", &cfg.GraphLimits.Writes); err != nil {
		return cfg, err
	}
	if err := loadDurationEnv("GRAPHDB_ROUTER_GRAPH_QUEUE_WAIT", &cfg.GraphLimits.Wait); err != nil {
		return cfg, err
	}
	if err := cfg.GraphLimits.Validate(); err != nil {
		return cfg, err
	}
	return cfg, cfg.Catalog.Validate()
}
