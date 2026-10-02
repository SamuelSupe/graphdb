package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type RaftConfig struct {
	Protocol            int
	Enabled             bool
	ID                  uint64
	ClusterID           string
	Addr                string
	Dir                 string
	Peers               map[uint64]string
	Token               string
	Bootstrap           bool
	Tick                time.Duration
	SnapshotEntries     uint64
	MaxSnapshotBytes    int64
	StreamSnapshots     bool
	ShardID             string
	Catalog             bool
	AllowLegacyProtocol bool
}

func loadRaftConfig(dataDir string) (RaftConfig, error) {
	cfg := RaftConfig{Bootstrap: true, Tick: 100 * time.Millisecond, SnapshotEntries: 1000, MaxSnapshotBytes: 512 << 20}
	raw := strings.TrimSpace(os.Getenv("GRAPHDB_RAFT_NODE_ID"))
	if raw == "" {
		for _, key := range []string{"GRAPHDB_RAFT_CLUSTER_ID", "GRAPHDB_RAFT_ADDR", "GRAPHDB_RAFT_PEERS", "GRAPHDB_RAFT_TOKEN", "GRAPHDB_RAFT_DIR", "GRAPHDB_RAFT_BOOTSTRAP", "GRAPHDB_RAFT_TICK", "GRAPHDB_RAFT_SNAPSHOT_ENTRIES", "GRAPHDB_RAFT_MAX_SNAPSHOT_BYTES", "GRAPHDB_RAFT_SHARD_ID", "GRAPHDB_RAFT_CATALOG", "GRAPHDB_RAFT_ALLOW_LEGACY_PROTOCOL", "GRAPHDB_RAFT_STREAM_SNAPSHOTS", "GRAPHDB_RAFT_PROTOCOL_VERSION"} {
			if os.Getenv(key) != "" {
				return cfg, fmt.Errorf("GRAPHDB_RAFT_NODE_ID is required with %s", key)
			}
		}
		return cfg, nil
	}
	cfg.Enabled = true
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return cfg, fmt.Errorf("GRAPHDB_RAFT_NODE_ID must be a positive integer")
	}
	cfg.ID = id
	cfg.ShardID = strings.TrimSpace(os.Getenv("GRAPHDB_RAFT_SHARD_ID"))
	if cfg.ShardID != "" && (strings.ContainsAny(cfg.ShardID, "/\\ \t\r\n") || cfg.ShardID == "." || cfg.ShardID == ".." || len(cfg.ShardID) > 128) {
		return cfg, fmt.Errorf("invalid GRAPHDB_RAFT_SHARD_ID")
	}
	if err := loadBoolEnv("GRAPHDB_RAFT_CATALOG", &cfg.Catalog); err != nil {
		return cfg, err
	}
	if cfg.Catalog && cfg.ShardID != "" {
		return cfg, fmt.Errorf("a Raft group must be either catalog or data shard")
	}
	cfg.ClusterID = strings.TrimSpace(os.Getenv("GRAPHDB_RAFT_CLUSTER_ID"))
	if cfg.ClusterID == "" || strings.ContainsAny(cfg.ClusterID, "/\\ \t\r\n") {
		return cfg, fmt.Errorf("GRAPHDB_RAFT_CLUSTER_ID must be a nonempty identifier")
	}
	cfg.Addr = strings.TrimSpace(os.Getenv("GRAPHDB_RAFT_ADDR"))
	if _, _, err := net.SplitHostPort(cfg.Addr); err != nil {
		return cfg, fmt.Errorf("GRAPHDB_RAFT_ADDR must be host:port")
	}
	cfg.Token = os.Getenv("GRAPHDB_RAFT_TOKEN")
	if len(cfg.Token) < 32 {
		return cfg, fmt.Errorf("GRAPHDB_RAFT_TOKEN must contain at least 32 bytes")
	}
	cfg.Dir = getenv("GRAPHDB_RAFT_DIR", filepath.Join(dataDir, ".graphdb-raft"))
	if err := json.Unmarshal([]byte(os.Getenv("GRAPHDB_RAFT_PEERS")), &cfg.Peers); err != nil {
		return cfg, fmt.Errorf("GRAPHDB_RAFT_PEERS must be a JSON object of node IDs to private HTTP origins: %w", err)
	}
	if len(cfg.Peers) < 3 {
		return cfg, fmt.Errorf("Raft requires at least three peer addresses")
	}
	addresses := make(map[string]bool)
	for id, address := range cfg.Peers {
		parsed, err := url.Parse(address)
		if id == 0 || err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return cfg, fmt.Errorf("invalid Raft peer origin for node %d", id)
		}
		if addresses[address] {
			return cfg, fmt.Errorf("Raft peer origins must be unique")
		}
		addresses[address] = true
	}
	if cfg.Peers[cfg.ID] == "" {
		return cfg, fmt.Errorf("Raft peers must contain this node's ID")
	}
	if err := loadBoolEnv("GRAPHDB_RAFT_BOOTSTRAP", &cfg.Bootstrap); err != nil {
		return cfg, err
	}
	if err := loadBoolEnv("GRAPHDB_RAFT_ALLOW_LEGACY_PROTOCOL", &cfg.AllowLegacyProtocol); err != nil {
		return cfg, err
	}
	if err := loadBoolEnv("GRAPHDB_RAFT_STREAM_SNAPSHOTS", &cfg.StreamSnapshots); err != nil {
		return cfg, err
	}
	cfg.Protocol = 1
	if err := loadIntEnv("GRAPHDB_RAFT_PROTOCOL_VERSION", &cfg.Protocol); err != nil {
		return cfg, err
	}
	if cfg.Protocol < 1 || cfg.Protocol > 2 {
		return cfg, fmt.Errorf("Raft protocol version must be 1 or 2")
	}
	if err := loadDurationEnv("GRAPHDB_RAFT_TICK", &cfg.Tick); err != nil {
		return cfg, err
	}
	if cfg.Tick < 10*time.Millisecond {
		return cfg, fmt.Errorf("Raft tick must be at least 10ms")
	}
	if raw := os.Getenv("GRAPHDB_RAFT_SNAPSHOT_ENTRIES"); raw != "" {
		count, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || count == 0 {
			return cfg, fmt.Errorf("Raft snapshot entries must be positive")
		}
		cfg.SnapshotEntries = count
	}
	if err := loadBytesEnv("GRAPHDB_RAFT_MAX_SNAPSHOT_BYTES", &cfg.MaxSnapshotBytes); err != nil {
		return cfg, err
	}
	if cfg.MaxSnapshotBytes <= 0 || cfg.MaxSnapshotBytes > 1<<40 {
		return cfg, fmt.Errorf("Raft snapshot budget must be positive and cannot exceed 1 TiB")
	}
	return cfg, nil
}
