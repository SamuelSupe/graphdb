package partition

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

type queryCursor struct {
	Pin    string `json:"pin"`
	Cursor string `json:"cursor"`
}

func ExecutePinnedQuery(ctx context.Context, remote Remote, home Group, g Graph, request query.Request) (query.Response, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Response{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, MaxQueryLifetime)
	defer cancel()
	envelope := queryCursor{}
	if request.Cursor != "" {
		if len(request.Cursor) > 16384 || !strings.HasPrefix(request.Cursor, "pg1.") {
			return query.Response{}, fmt.Errorf("%w: invalid partitioned graph cursor", query.ErrInvalid)
		}
		data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(request.Cursor, "pg1."))
		if err != nil || json.Unmarshal(data, &envelope) != nil || !validTokenID(envelope.Pin) {
			return query.Response{}, fmt.Errorf("%w: invalid partitioned graph cursor", query.ErrInvalid)
		}
	}
	signatureRequest := request
	signatureRequest.Cursor = ""
	signature, _ := json.Marshal(signatureRequest)
	digest := fmt.Sprintf("%x", sha256.Sum256(signature))
	pinned, err := remote.Pin(ctx, home, g.Tenant, g.Generation, g.Version, digest, envelope.Pin)
	if err != nil {
		return query.Response{}, err
	}
	request.Cursor = envelope.Cursor
	response, err := ExecuteQuery(ctx, View{Graph: pinned.Graph, Remote: remote}, request)
	if envelope.Pin == "" && (err != nil || response.NextCursor == "") {
		_ = remote.Action(ctx, home, Action{Operation: "release_pin", Tenant: g.Tenant, Generation: g.Generation, Txn: pinned.Pin.ID, Digest: digest}, nil)
	}
	if err != nil {
		return query.Response{}, err
	}
	if response.NextCursor != "" {
		data, _ := json.Marshal(queryCursor{Pin: pinned.Pin.ID, Cursor: response.NextCursor})
		response.NextCursor = "pg1." + base64.RawURLEncoding.EncodeToString(data)
	}
	return response, nil
}
