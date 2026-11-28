package partition

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const logicalDigestCacheBytes = 64 << 20

// LogicalDigestCache borrows validated, ordered digests from immutable leaves.
// Snapshots and regular tree reads always read and validate the original pages.
type LogicalDigestCache struct {
	mu    sync.Mutex
	pages map[logicalDigestKey]*list.Element
	order list.List
	bytes int
}

type logicalDigestKey struct {
	path string
	root Root
}

type logicalDigestPage struct {
	key       logicalDigestKey
	file      os.FileInfo
	validated time.Time
	digests   []byte
	size      int
}

func (c *LogicalDigestCache) get(key logicalDigestKey) logicalDigestPage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.pages[key]; elem != nil {
		c.order.MoveToBack(elem)
		return elem.Value.(logicalDigestPage)
	}
	return logicalDigestPage{}
}

func (c *LogicalDigestCache) put(key logicalDigestKey, info os.FileInfo, digests []byte) {
	page := logicalDigestPage{key: key, file: info, digests: bytes.Clone(digests), validated: time.Now(),
		size: len(digests) + len(key.path) + len(key.root.Hash) + len(key.root.First) + len(key.root.Last) + 512}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pages == nil {
		c.pages = make(map[logicalDigestKey]*list.Element)
	}
	if previous := c.pages[key]; previous != nil {
		c.bytes -= previous.Value.(logicalDigestPage).size
		c.order.Remove(previous)
	}
	c.pages[key] = c.order.PushBack(page)
	c.bytes += page.size
	for c.bytes > logicalDigestCacheBytes {
		oldest := c.order.Front()
		entry := oldest.Value.(logicalDigestPage)
		c.bytes -= entry.size
		delete(c.pages, entry.key)
		c.order.Remove(oldest)
	}
}

type immutablePageReader interface {
	GetIfFileUnchanged(context.Context, string, os.FileInfo) ([]byte, os.FileInfo, error)
}

func (t Tree) LogicalBuckets(ctx context.Context, root Root, changes []Row) (map[int]string, error) {
	result := map[int]string{}
	var reader immutablePageReader
	if t.logicalDigests != nil {
		reader, _ = t.Objects.(immutablePageReader)
		if reader != nil {
			ctx = storage.WithPreparedFileParents(ctx, t.Objects)
		}
	}
	for _, index := range logicalBucketsForChanges(changes) {
		digest := sha256.New()
		count, err := t.writeLogicalRange(ctx, reader, root, LogicalPrefix(index/256, index%256), digest, 0)
		if err != nil {
			return nil, err
		}
		value := ""
		if count != 0 {
			value = hex.EncodeToString(digest.Sum(nil))
		}
		result[index] = value
	}
	return result, nil
}

func (t Tree) writeLogicalRange(ctx context.Context, reader immutablePageReader, ref Root, prefix string, digest hash.Hash, depth int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if ref.Hash == "" || ref.Last < prefix || (ref.First >= prefix && !strings.HasPrefix(ref.First, prefix)) {
		return 0, nil
	}
	if depth > 32 || !ref.validPageRef() {
		return 0, fmt.Errorf("%w: invalid logical fragment path", ErrPartitionCorrupt)
	}
	var page treePage
	var key logicalDigestKey
	var file os.FileInfo
	var err error
	if reader != nil && ref.Count <= pageEntries && strings.HasPrefix(ref.First, prefix) && strings.HasPrefix(ref.Last, prefix) {
		key = logicalDigestKey{path: t.pageKey(ref.Hash), root: ref}
		cached := t.logicalDigests.get(key)
		data, info, readErr := reader.GetIfFileUnchanged(ctx, key.path, cached.file)
		if readErr == nil && info != nil && data == nil && cached.file != nil {
			_, _ = digest.Write(cached.digests)
			return ref.Count, nil
		}
		page, _, err = decodeTreePage(ref, data, readErr)
		file = info
	} else {
		page, err = t.load(ctx, ref)
	}
	if err != nil {
		return 0, err
	}
	if len(page.Children) != 0 {
		var count int64
		for _, child := range page.Children {
			rows, err := t.writeLogicalRange(ctx, reader, child, prefix, digest, depth+1)
			if err != nil {
				return 0, err
			}
			count += rows
		}
		return count, nil
	}
	var decoded [pageEntries * sha256.Size]byte
	count := 0
	for _, row := range page.Rows {
		if !strings.HasPrefix(row.Key, prefix) {
			continue
		}
		if err := decodeLogicalDigest(decoded[count*sha256.Size:(count+1)*sha256.Size], row.Value); err != nil {
			return 0, err
		}
		count++
	}
	ordered := decoded[:count*sha256.Size]
	_, _ = digest.Write(ordered)
	if file != nil {
		t.logicalDigests.put(key, file, ordered)
	}
	return int64(count), nil
}

func decodeLogicalDigest(dst, value []byte) error {
	// Normal leaves are canonical JSON strings with unescaped hex digits.
	if len(value) == sha256.Size*2+2 && value[0] == '"' && value[len(value)-1] == '"' {
		if _, err := hex.Decode(dst, value[1:len(value)-1]); err == nil {
			return nil
		}
	}
	var text string
	if err := json.Unmarshal(value, &text); err != nil {
		return err
	}
	if len(text) == sha256.Size*2 {
		if _, err := hex.Decode(dst, []byte(text)); err == nil {
			return nil
		}
	}
	return fmt.Errorf("invalid stored logical leaf")
}
