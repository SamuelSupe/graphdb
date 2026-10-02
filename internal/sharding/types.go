package sharding

import (
	"fmt"
	"net/url"
	"strings"
)

const EpochHeader = "X-GraphDB-Route-Epoch"
const TargetEpochHeader = "X-GraphDB-Target-Route-Epoch"
const ChunkBytes = 1 << 20

type Shard struct {
	ID        string            `json:"id"`
	ClusterID string            `json:"cluster_id"`
	Peers     map[uint64]string `json:"peers"`
	Draining  bool              `json:"draining,omitempty"`
}

type Move struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
	Epoch  uint64 `json:"epoch"`
	Phase  string `json:"phase"`
	Error  string `json:"error,omitempty"`
}

type Placement struct {
	Tenant string `json:"tenant_id"`
	Shard  string `json:"shard_id"`
	Epoch  uint64 `json:"epoch"`
	State  string `json:"state"`
	Move   *Move  `json:"move,omitempty"`
}

type Catalog struct {
	Version uint64               `json:"version"`
	Shards  map[string]Shard     `json:"shards"`
	Tenants map[string]Placement `json:"tenants"`
}

type Resolution struct {
	Placement Placement `json:"placement"`
	Shard     Shard     `json:"shard"`
}

type Ownership struct {
	Epoch    uint64 `json:"epoch"`
	State    string `json:"state"`
	MoveID   string `json:"move_id,omitempty"`
	Digest   string `json:"digest,omitempty"`
	NextPart int    `json:"next_part,omitempty"`
}

type Action struct {
	Operation string `json:"operation"`
	Tenant    string `json:"tenant_id,omitempty"`
	Shard     *Shard `json:"shard,omitempty"`
	Target    string `json:"target,omitempty"`
	Epoch     uint64 `json:"epoch,omitempty"`
	MoveID    string `json:"move_id,omitempty"`
	Phase     string `json:"phase,omitempty"`
	Error     string `json:"error,omitempty"`
	Part      int    `json:"part,omitempty"`
	Parts     int    `json:"parts,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	Digest    string `json:"digest,omitempty"`
	Data      []byte `json:"data,omitempty"`
}

type Object struct {
	Key  string `json:"key"`
	Data []byte `json:"data"`
}

type Transfer struct {
	Tenant  string   `json:"tenant_id"`
	MoveID  string   `json:"move_id"`
	Objects []Object `json:"objects"`
}

type TransferInfo struct {
	Bytes  int64  `json:"bytes"`
	Digest string `json:"digest"`
	Parts  int    `json:"parts"`
}

func ValidateIdentifier(id string) error {
	if id == "" || len(id) > 128 || strings.ContainsAny(id, "/\\ \t\r\n") || id == "." || id == ".." {
		return fmt.Errorf("invalid shard or cluster identifier")
	}
	return nil
}

func (s Shard) Validate() error {
	if err := ValidateIdentifier(s.ID); err != nil {
		return err
	}
	if err := ValidateIdentifier(s.ClusterID); err != nil {
		return err
	}
	if len(s.Peers) < 3 {
		return fmt.Errorf("a shard requires at least three Raft peer addresses")
	}
	seen := make(map[string]bool)
	for id, origin := range s.Peers {
		u, err := url.Parse(origin)
		if id == 0 || err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return fmt.Errorf("shard peer must be a private HTTP origin")
		}
		canonical := strings.TrimRight(origin, "/")
		if seen[canonical] {
			return fmt.Errorf("shard peer addresses must be unique")
		}
		seen[canonical] = true
	}
	return nil
}
