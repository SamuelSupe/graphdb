package ha

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (c *Cluster) servePartitionedGraph(w http.ResponseWriter, r *http.Request, body []byte, tenant string) bool {
	if tenant == "" || c.App.ShardID == "" || c.config.Protocol < 5 {
		return false
	}
	c.App.mu.RLock()
	g, err := c.App.partitionStore().GetGraph(r.Context(), tenant)
	c.App.mu.RUnlock()
	if errors.Is(err, storage.ErrNotFound) {
		return false
	}
	if err != nil {
		c.partitionError(w, err)
		return true
	}
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	if sharding.ServeGraphData(w, r, g, c.graphHome(), c.shards) {
		return true
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/tenants/"+tenant {
		info, err := c.App.Store.GetTenantInfo(r.Context(), tenant)
		if err != nil {
			c.partitionError(w, err)
			return true
		}
		info.ManifestVersion = g.Version
		info.SnapshotVersion = g.Version
		info.CommitTailLength = 0
		data, _ := resultJSON(http.StatusOK, info)
		writeResult(w, data)
		return true
	}
	data, _ := resultJSON(http.StatusNotImplemented, map[string]any{"code": "operation_disabled", "error": "operation is not available for partitioned graphs", "retryable": false})
	writeResult(w, data)
	return true
}
