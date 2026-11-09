package partition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

type queryCursor struct {
	Expires int64  `json:"expires,omitempty"`
	Pin     string `json:"pin"`
	Query   string `json:"query,omitempty"`
	Cursor  string `json:"cursor"`
}

func ExecutePinnedQuery(ctx context.Context, remote Remote, home Group, g Graph, request query.Request) (query.Response, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Response{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, MaxQueryLifetime)
	defer cancel()
	signatureRequest := request
	signatureRequest.Cursor = ""
	signature, _ := json.Marshal(signatureRequest)
	digest := fmt.Sprintf("%x", sha256.Sum256(signature))
	envelope := queryCursor{}
	var pinned PinnedView
	var err error
	if request.Cursor == "" {
		if g.Version < request.MinVersion {
			return query.Response{}, ErrVersionNotReady
		}
		pinned, err = AcquireReadView(ctx, remote, home, g)
	} else {
		prefix, encoded, ok := strings.Cut(request.Cursor, ".")
		if len(request.Cursor) > 16384 || !ok || prefix != "pg1" && prefix != "pg2" {
			return query.Response{}, fmt.Errorf("%w: invalid partitioned graph cursor", query.ErrInvalid)
		}
		data, decodeErr := base64.RawURLEncoding.DecodeString(encoded)
		if decodeErr != nil || json.Unmarshal(data, &envelope) != nil || !validTokenID(envelope.Pin) ||
			envelope.Cursor == "" {
			return query.Response{}, fmt.Errorf("%w: invalid partitioned graph cursor", query.ErrInvalid)
		}
		pinQuery := digest
		if prefix == "pg2" {
			if envelope.Query != digest || time.Now().Unix() >= envelope.Expires {
				return query.Response{}, invalid("graph cursor expired or does not match the query")
			}
			pinQuery = ""
		}
		pinned, err = remote.Pin(ctx, home, g.Tenant, g.Generation, g.Version, pinQuery, envelope.Pin)
		if err == nil && time.Now().Add(MaxQueryLifetime).After(pinned.Pin.ExpiresAt) {
			action := Action{Operation: "pin", Revision: ActionRevision, Tenant: g.Tenant, Generation: g.Generation, BaseVersion: pinned.Pin.Version,
				Digest: pinQuery, Txn: envelope.Pin, At: time.Now().UTC()}
			if prefix == "pg2" {
				action.Purpose = "shared"
			}
			err = remote.Action(ctx, home, action, &pinned)
		}
	}
	if err != nil {
		return query.Response{}, err
	}
	request.Cursor = envelope.Cursor
	response, err := ExecuteQuery(ctx, View{Graph: pinned.Graph, Remote: remote}, request)
	if err != nil {
		return query.Response{}, err
	}
	if response.NextCursor != "" {
		next := queryCursor{Pin: pinned.Pin.ID, Cursor: response.NextCursor}
		prefix := "pg1."
		if pinned.Pin.Query == "" {
			prefix = "pg2."
			next.Query = digest
			next.Expires = pinned.Pin.ExpiresAt.Unix()
		}
		data, _ := json.Marshal(next)
		response.NextCursor = prefix + base64.RawURLEncoding.EncodeToString(data)
	}
	return response, nil
}
