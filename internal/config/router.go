package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
)

type RouterConfig struct {
	Addr    string
	Token   string
	Catalog sharding.Shard
}

func LoadRouter() (RouterConfig, error) {
	cfg := RouterConfig{Addr: getenv("GRAPHDB_ADDR", ":8080"), Token: os.Getenv("GRAPHDB_ROUTER_TOKEN"), Catalog: sharding.Shard{ID: "catalog", ClusterID: os.Getenv("GRAPHDB_ROUTER_CATALOG_CLUSTER_ID")}}
	if len(cfg.Token) < 32 {
		return cfg, fmt.Errorf("GRAPHDB_ROUTER_TOKEN must contain at least 32 bytes")
	}
	if err := json.Unmarshal([]byte(os.Getenv("GRAPHDB_ROUTER_CATALOG_PEERS")), &cfg.Catalog.Peers); err != nil {
		return cfg, fmt.Errorf("GRAPHDB_ROUTER_CATALOG_PEERS must map node IDs to private HTTP origins: %w", err)
	}
	return cfg, cfg.Catalog.Validate()
}
