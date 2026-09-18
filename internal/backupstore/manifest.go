package backupstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const Format = "graphdb-object-snapshot-v1"
const maxManifestBytes = 64 << 10

type Manifest struct {
	Format      string    `json:"format"`
	TenantID    string    `json:"tenant_id"`
	BackupID    string    `json:"backup_id"`
	Version     int64     `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	SnapshotKey string    `json:"snapshot_key"`
	Bytes       int64     `json:"bytes"`
	SHA256      string    `json:"sha256"`
}

type Entry struct {
	Manifest
	BackupKey string `json:"backup_key"`
}

type Page struct {
	Backups    []Entry `json:"backups"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func validID(id string) bool { return idPattern.MatchString(id) && !strings.Contains(id, "..") }

func (r *Repository) manifestKey(tenant, id string) string {
	return path.Join(r.prefix, tenant, id, "manifest.json")
}

func (r *Repository) URI(tenant, id string) (string, error) {
	if !validID(tenant) || !validID(id) {
		return "", fmt.Errorf("invalid backup identity")
	}
	return (&url.URL{Scheme: "s3", Host: r.bucket, Path: "/" + r.manifestKey(tenant, id)}).String(), nil
}

// ParseURI restricts requests to the configured repository. Request bodies cannot
// choose another bucket, endpoint, prefix, or arbitrary object within the bucket.
func (r *Repository) ParseURI(uri string) (tenant, id string, err error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host != r.bucket || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("backup_key must identify a snapshot in the configured S3 backup repository")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid object backup key")
	}
	tenant, id = parts[len(parts)-3], parts[len(parts)-2]
	canonical, err := r.URI(tenant, id)
	if err != nil || canonical != uri {
		return "", "", fmt.Errorf("invalid object backup key")
	}
	return tenant, id, nil
}

func (r *Repository) validate(m Manifest, tenant, id string) error {
	hash, err := hex.DecodeString(m.SHA256)
	if m.Format != Format || m.TenantID != tenant || m.BackupID != id || m.Version < 0 || m.CreatedAt.IsZero() || m.Bytes <= 0 || m.Bytes > 5<<40 || err != nil || len(hash) != sha256.Size || strings.ToLower(m.SHA256) != m.SHA256 {
		return fmt.Errorf("invalid object backup manifest")
	}
	if m.SnapshotKey != path.Join(r.prefix, tenant, id, m.SHA256+".parquet") {
		return fmt.Errorf("object backup snapshot key does not match its identity and checksum")
	}
	return nil
}

func (r *Repository) ReadManifest(ctx context.Context, uri string) (Manifest, error) {
	tenant, id, err := r.ParseURI(uri)
	if err != nil {
		return Manifest{}, err
	}
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(r.manifestKey(tenant, id))})
	if isAPIError(err, "NoSuchKey", "NotFound") {
		return Manifest{}, ErrNotFound
	}
	if err != nil {
		return Manifest{}, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(io.LimitReader(out.Body, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, err
	}
	if len(data) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("object backup manifest exceeds size limit")
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, err
	}
	if err := r.validate(m, tenant, id); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// Publish writes the snapshot before its manifest commit marker. Retries use a
// content-addressed snapshot and never replace a different published backup.
func (r *Repository) Publish(ctx context.Context, m Manifest, source *io.SectionReader) (Entry, error) {
	uri, err := r.URI(m.TenantID, m.BackupID)
	if err != nil {
		return Entry{}, err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, source)
	if err != nil {
		return Entry{}, err
	}
	m.Format, m.Bytes, m.SHA256 = Format, n, hex.EncodeToString(hash.Sum(nil))
	m.SnapshotKey = path.Join(r.prefix, m.TenantID, m.BackupID, m.SHA256+".parquet")
	if err := r.validate(m, m.TenantID, m.BackupID); err != nil {
		return Entry{}, err
	}
	if previous, err := r.ReadManifest(ctx, uri); err == nil {
		if previous != m {
			return Entry{}, fmt.Errorf("backup ID already contains a different snapshot")
		}
		return Entry{Manifest: m, BackupKey: uri}, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Entry{}, err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return Entry{}, err
	}
	uploader := manager.NewUploader(r.client, func(u *manager.Uploader) {
		u.PartSize = 16 << 20
		u.Concurrency = 2
	})
	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(m.SnapshotKey), Body: source,
		ContentType: aws.String("application/vnd.apache.parquet"),
	}); err != nil {
		var failed manager.MultiUploadFailure
		if ctx.Err() != nil && errors.As(err, &failed) {
			// The uploader aborts using its request context, which is already
			// canceled here. Give cleanup a separate, bounded chance to finish.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_, _ = r.client.AbortMultipartUpload(cleanup, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(r.bucket), Key: aws.String(m.SnapshotKey), UploadId: aws.String(failed.UploadID()),
			})
			cancel()
		}
		return Entry{}, err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return Entry{}, err
	}
	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.manifestKey(m.TenantID, m.BackupID)),
		Body: bytes.NewReader(data), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		// A lost response can leave a successfully published manifest. Only the
		// identical manifest makes that ambiguous outcome safe to resume.
		previous, readErr := r.ReadManifest(ctx, uri)
		if readErr != nil || previous != m {
			return Entry{}, err
		}
	}
	return Entry{Manifest: m, BackupKey: uri}, nil
}

// Download validates length and SHA-256 while streaming into caller-owned staging.
// The caller must not apply the snapshot unless this method succeeds.
func (r *Repository) Download(ctx context.Context, m Manifest, dst io.Writer) error {
	if err := r.validate(m, m.TenantID, m.BackupID); err != nil {
		return err
	}
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(m.SnapshotKey)})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	if out.ContentLength == nil || *out.ContentLength != m.Bytes {
		return fmt.Errorf("backup snapshot length mismatch")
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, hash), io.LimitReader(out.Body, m.Bytes+1))
	if err != nil {
		return err
	}
	if n != m.Bytes || hex.EncodeToString(hash.Sum(nil)) != m.SHA256 {
		return fmt.Errorf("backup snapshot checksum or length mismatch")
	}
	return ctx.Err()
}

func (r *Repository) List(ctx context.Context, tenant, cursor string, limit int) (Page, error) {
	if !validID(tenant) {
		return Page{}, fmt.Errorf("invalid backup tenant")
	}
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > 100 {
		return Page{}, fmt.Errorf("backup list limit must be between 1 and 100")
	}
	prefix := path.Join(r.prefix, tenant) + "/"
	start := ""
	if cursor != "" {
		cursorTenant, id, err := r.ParseURI(cursor)
		if err != nil || cursorTenant != tenant {
			return Page{}, fmt.Errorf("invalid backup cursor")
		}
		start = r.manifestKey(tenant, id)
	}
	page := Page{Backups: []Entry{}}
	var token *string
	for {
		out, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucket), Prefix: aws.String(prefix), StartAfter: aws.String(start), ContinuationToken: token, MaxKeys: aws.Int32(100),
		})
		if err != nil {
			return Page{}, err
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
			if len(parts) != 2 || parts[1] != "manifest.json" || !validID(parts[0]) {
				continue
			}
			if len(page.Backups) == limit {
				page.NextCursor = page.Backups[len(page.Backups)-1].BackupKey
				return page, nil
			}
			uri, _ := r.URI(tenant, parts[0])
			m, err := r.ReadManifest(ctx, uri)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return Page{}, err
			}
			page.Backups = append(page.Backups, Entry{Manifest: m, BackupKey: uri})
		}
		if !aws.ToBool(out.IsTruncated) {
			return page, nil
		}
		if aws.ToString(out.NextContinuationToken) == "" || (token != nil && *token == *out.NextContinuationToken) {
			return Page{}, fmt.Errorf("S3 returned an invalid continuation token")
		}
		token = out.NextContinuationToken
	}
}
