package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type metadataEnvelope struct {
	Digest string          `json:"digest"`
	Data   json.RawMessage `json:"data"`
}

func (s Store) load(ctx context.Context, key string, value any) error {
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	var envelope metadataEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if !validDigest(envelope.Digest) || fmt.Sprintf("%x", sha256.Sum256(envelope.Data)) != envelope.Digest {
		return fmt.Errorf("graph metadata checksum mismatch in %q", key)
	}
	return json.Unmarshal(envelope.Data, value)
}

func (s Store) save(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(metadataEnvelope{Digest: fmt.Sprintf("%x", sha256.Sum256(data)), Data: data})
	if err != nil {
		return err
	}
	return s.Objects.Put(ctx, key, encoded)
}
