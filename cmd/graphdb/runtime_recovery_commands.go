package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const runtimeRestoreJournal = ".graphdb-runtime-restore.json"

type runtimeArchiveManifest struct {
	Format     int      `json:"format"`
	Prefix     string   `json:"prefix"`
	IngestMode string   `json:"ingest_mode"`
	NodeID     uint64   `json:"node_id,omitempty"`
	ClusterID  string   `json:"cluster_id,omitempty"`
	ShardID    string   `json:"shard_id,omitempty"`
	Catalog    bool     `json:"catalog,omitempty"`
	Roles      []string `json:"roles"`
}

type runtimeRestorePlan struct {
	SHA256 string   `json:"sha256"`
	Files  []string `json:"files"`
}

func runtimeArchiveRoots(cfg config.Config) (map[string]string, runtimeArchiveManifest, error) {
	roots := map[string]string{"data": cfg.DataDir}
	manifest := runtimeArchiveManifest{Format: 1, Prefix: cfg.Prefix, IngestMode: cfg.IngestMode, NodeID: cfg.Raft.ID, ClusterID: cfg.Raft.ClusterID, ShardID: cfg.Raft.ShardID, Catalog: cfg.Raft.Catalog}
	if cfg.Raft.Enabled {
		roots["raft"] = cfg.Raft.Dir
	} else if cfg.IngestMode == "wal" {
		roots["wal"] = cfg.IngestWALDir
	}
	for role, root := range roots {
		absolute, err := filepath.Abs(root)
		if err != nil {
			return nil, manifest, err
		}
		if absolute == string(filepath.Separator) {
			return nil, manifest, fmt.Errorf("runtime recovery root cannot be the filesystem root")
		}
		roots[role] = absolute
		manifest.Roles = append(manifest.Roles, role)
	}
	for role, root := range roots {
		for other, otherRoot := range roots {
			if role != other && root == otherRoot {
				return nil, manifest, fmt.Errorf("runtime recovery roots must be distinct")
			}
		}
	}
	sort.Strings(manifest.Roles)
	return roots, manifest, nil
}

func runRuntimeRecovery(args []string, cfg config.Config) error {
	if len(args) != 2 && len(args) != 4 {
		return fmt.Errorf("usage: graphdb %s <archive> [--max-bytes <bytes>]", args[0])
	}
	limit := int64(1 << 40)
	if len(args) == 4 {
		if args[2] != "--max-bytes" {
			return fmt.Errorf("unknown runtime recovery option")
		}
		var err error
		limit, err = strconv.ParseInt(args[3], 10, 64)
		if err != nil || limit < 4096 || limit > 1<<40 {
			return fmt.Errorf("max-bytes must be between 4096 and 1099511627776")
		}
	}
	roots, manifest, err := runtimeArchiveRoots(cfg)
	if err != nil {
		return err
	}
	archive, err := filepath.Abs(args[1])
	if err != nil {
		return err
	}
	for _, root := range roots {
		relative, _ := filepath.Rel(root, archive)
		if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("runtime archive must be outside the restored roots")
		}
	}
	if args[0] == "runtime-backup" {
		return backupRuntime(archive, roots, manifest, limit)
	}
	return restoreRuntime(archive, roots, manifest, limit, storage.DiskSpacePolicy{MinFreeBytes: cfg.DiskMinFreeBytes, MinFreePercent: cfg.DiskMinFreePercent})
}

func backupRuntime(filename string, roots map[string]string, manifest runtimeArchiveManifest, limit int64) (err error) {
	files, err := storage.OpenFileStore(roots["data"])
	if err != nil {
		return err
	}
	defer files.Close()
	if _, err := os.Lstat(filename); !os.IsNotExist(err) {
		return fmt.Errorf("backup target already exists or cannot be inspected")
	}
	output, err := os.CreateTemp(filepath.Dir(filename), ".runtime-backup-")
	if err != nil {
		return err
	}
	defer func() { output.Close(); os.Remove(output.Name()) }()
	if _, err := output.Write(make([]byte, sha256.Size)); err != nil {
		return err
	}
	digest := sha256.New()
	compressed := gzip.NewWriter(io.MultiWriter(output, digest))
	archive := tar.NewWriter(&runtimeBudgetWriter{writer: compressed, remaining: limit})
	metadata, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := archive.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0600, Size: int64(len(metadata))}); err != nil {
		return err
	}
	if _, err := archive.Write(metadata); err != nil {
		return err
	}
	var total int64
	for _, role := range manifest.Roles {
		root := roots[role]
		err = filepath.WalkDir(root, func(filename string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if filename == root {
				if !entry.IsDir() {
					return fmt.Errorf("runtime archive root is not a directory: %s", root)
				}
				return nil
			}
			for other, otherRoot := range roots {
				if role != other && filename == otherRoot && entry.IsDir() {
					return filepath.SkipDir
				}
			}
			relative, err := filepath.Rel(root, filename)
			if err != nil {
				return err
			}
			name := entry.Name()
			rootEntry := filepath.Dir(relative) == "."
			staging := rootEntry && (strings.HasPrefix(name, ".snapshot-view-") || strings.HasPrefix(name, ".graphdb-replication-snapshot-") || strings.HasPrefix(name, ".maintenance-") || strings.HasPrefix(name, ".runtime-restore-"))
			temporary := !entry.IsDir() && ((strings.HasPrefix(name, ".tmp-") && !strings.HasSuffix(name, ".parquet")) || (role == "raft" && (strings.HasPrefix(name, ".build-") || strings.HasPrefix(name, ".snapshot-"))))
			if staging || temporary {
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if entry.IsDir() {
				return nil
			}
			if rootEntry && (name == ".graphdb.lock" || name == runtimeRestoreJournal) {
				return nil
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("non-regular runtime archive object: %s", filename)
			}
			source, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer source.Close()
			info, err := source.Stat()
			if err != nil {
				return err
			}
			if info.Size() > limit-total {
				return fmt.Errorf("runtime archive exceeds uncompressed byte budget")
			}
			total += info.Size()
			if err := archive.WriteHeader(&tar.Header{Name: role + "/" + filepath.ToSlash(relative), Size: info.Size(), Mode: 0600}); err != nil {
				return err
			}
			_, err = io.Copy(archive, source)
			return err
		})
		if err != nil {
			return err
		}
	}
	if err := archive.Close(); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := output.Write(digest.Sum(nil)); err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := os.Link(output.Name(), filename); err != nil {
		return err
	}
	return syncRuntimeDirectory(filepath.Dir(filename))
}

func runtimeArchiveName(name string, roots map[string]string) (string, string, error) {
	role, relative, ok := strings.Cut(name, "/")
	if !ok || roots[role] == "" || relative == "" || strings.Contains(relative, "\\") || strings.HasPrefix(relative, "/") || filepath.ToSlash(filepath.Clean(relative)) != relative {
		return "", "", fmt.Errorf("invalid runtime archive path")
	}
	for _, part := range strings.Split(relative, "/") {
		if part == ".." || part == "." {
			return "", "", fmt.Errorf("reserved runtime archive path")
		}
	}
	root, _, _ := strings.Cut(relative, "/")
	if root == ".graphdb.lock" || root == runtimeRestoreJournal || strings.HasPrefix(root, ".runtime-restore-") {
		return "", "", fmt.Errorf("reserved runtime archive path")
	}
	return role, relative, nil
}

func restoreRuntime(filename string, roots map[string]string, expected runtimeArchiveManifest, limit int64, policy storage.DiskSpacePolicy) (err error) {
	input, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer input.Close()
	var checksum [sha256.Size]byte
	if _, err := io.ReadFull(input, checksum[:]); err != nil {
		return err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, input); err != nil {
		return err
	}
	if !bytes.Equal(checksum[:], digest.Sum(nil)) {
		return fmt.Errorf("runtime archive checksum mismatch")
	}
	plan := runtimeRestorePlan{SHA256: hex.EncodeToString(checksum[:])}
	contents, err := inspectRuntimeArchive(input, roots, expected, limit)
	if err != nil {
		return err
	}
	plan.Files = contents.Files
	stages := make(map[string]string)
	for role, root := range roots {
		stages[role] = filepath.Join(root, ".runtime-restore-"+plan.SHA256[:16])
	}
	requiredByFS := map[string]int64{}
	for role, required := range contents.Usage {
		path := roots[role]
		for {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				path = filepath.Dir(path)
				continue
			}
			break
		}
		status, err := storage.InspectDiskSpace(context.Background(), path, policy)
		if err != nil {
			return err
		}
		requiredByFS[status.FilesystemID] += required
		if status.AvailableBytes-status.MinimumFreeBytes < requiredByFS[status.FilesystemID] {
			return fmt.Errorf("insufficient disk space to stage runtime restore for %s", role)
		}
	}
	files, err := storage.OpenFileStoreForRecovery(roots["data"])
	if err != nil {
		return err
	}
	defer files.Close()
	journal := filepath.Join(roots["data"], runtimeRestoreJournal)
	if info, statErr := os.Lstat(journal); statErr == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("restore journal is not a regular file")
		}
		data, err := os.ReadFile(journal)
		if err != nil {
			return err
		}
		var previous runtimeRestorePlan
		if json.Unmarshal(data, &previous) != nil || previous.SHA256 != plan.SHA256 || !slices.Equal(previous.Files, contents.Files) {
			return fmt.Errorf("interrupted restore requires its original archive and file plan")
		}
		if err := checkRuntimeTargets(roots, journal, contents.Files, stages); err != nil {
			return err
		}
		for _, name := range previous.Files {
			role, relative, _ := runtimeArchiveName(name, roots)
			if err := os.Remove(filepath.Join(roots[role], filepath.FromSlash(relative))); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		for _, stage := range stages {
			if err := os.RemoveAll(stage); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	if err := checkRuntimeTargets(roots, journal, nil, nil); err != nil {
		return err
	}
	createdStages := map[string]bool{}
	defer func() {
		for stage := range createdStages {
			os.RemoveAll(stage)
		}
	}()
	for _, role := range expected.Roles {
		if err := os.MkdirAll(roots[role], 0700); err != nil {
			return err
		}
		if _, err := os.Lstat(stages[role]); !os.IsNotExist(err) {
			return fmt.Errorf("restore staging target already exists or cannot be inspected")
		}
	}
	if err := writeRuntimeRestorePlan(journal, plan); err != nil {
		return err
	}
	for _, stage := range stages {
		if err := os.Mkdir(stage, 0700); err != nil {
			return err
		}
		createdStages[stage] = true
	}
	if _, err := input.Seek(sha256.Size, io.SeekStart); err != nil {
		return err
	}
	stagedDigest := sha256.New()
	compressed, err := gzip.NewReader(io.TeeReader(input, stagedDigest))
	if err != nil {
		return err
	}
	defer compressed.Close()
	limited := &io.LimitedReader{R: compressed, N: limit + 1}
	archive := tar.NewReader(limited)
	header, err := archive.Next()
	if err != nil || header.Name != "manifest.json" || header.Typeflag != tar.TypeReg || header.Size > 1<<20 {
		return fmt.Errorf("invalid runtime archive manifest")
	}
	metadata, err := io.ReadAll(archive)
	if err != nil {
		return err
	}
	var manifest runtimeArchiveManifest
	if json.Unmarshal(metadata, &manifest) != nil {
		return fmt.Errorf("invalid runtime archive manifest")
	}
	actual, _ := json.Marshal(manifest)
	wanted, _ := json.Marshal(expected)
	if !bytes.Equal(actual, wanted) {
		return fmt.Errorf("runtime archive deployment mode or Raft identity differs from the target")
	}
	seen := make(map[string]bool)
	usage := make(map[string]int64)
	var stagedFiles []string
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		role, relative, err := runtimeArchiveName(header.Name, roots)
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > limit || seen[header.Name] {
			return fmt.Errorf("invalid or duplicate runtime archive object")
		}
		seen[header.Name] = true
		stagedFiles = append(stagedFiles, header.Name)
		usage[role] += header.Size
		destination := filepath.Join(stages[role], filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return err
		}
		target, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		copied, copyErr := io.Copy(target, archive)
		err = errors.Join(copyErr, target.Sync(), target.Close())
		if err != nil {
			return err
		}
		if copied != header.Size {
			return fmt.Errorf("truncated runtime archive object")
		}
	}
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return err
	}
	if limited.N == 0 {
		return fmt.Errorf("runtime archive exceeds uncompressed byte budget")
	}
	if !bytes.Equal(checksum[:], stagedDigest.Sum(nil)) || !slices.Equal(stagedFiles, contents.Files) {
		return fmt.Errorf("runtime archive changed while staging restore")
	}
	// Each staging directory is on its destination filesystem; check the
	// remaining admission reserve before making any staged file visible.
	for role := range usage {
		status, err := storage.InspectDiskSpace(context.Background(), roots[role], policy)
		if err == nil && !status.WriteReady {
			err = fmt.Errorf("insufficient disk reserve to publish runtime restore")
		}
		if err != nil {
			return err
		}
	}
	directories := map[string]bool{}
	for _, name := range plan.Files {
		role, relative, _ := runtimeArchiveName(name, roots)
		destination := filepath.Join(roots[role], filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(stages[role], filepath.FromSlash(relative)), destination); err != nil {
			return err
		}
		for dir := filepath.Dir(destination); ; dir = filepath.Dir(dir) {
			directories[dir] = true
			if dir == roots[role] {
				break
			}
		}
	}
	sorted := make([]string, 0, len(directories))
	for dir := range directories {
		sorted = append(sorted, dir)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(sorted)))
	for _, dir := range sorted {
		if err := syncRuntimeDirectory(dir); err != nil {
			return err
		}
	}
	if err := os.Remove(journal); err != nil {
		return err
	}
	return syncRuntimeDirectory(roots["data"])
}

func syncRuntimeDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func checkRuntimeTargets(roots map[string]string, journal string, planned []string, stages map[string]string) error {
	allowed := map[string]bool{}
	for _, name := range planned {
		role, relative, err := runtimeArchiveName(name, roots)
		if err != nil {
			return err
		}
		allowed[filepath.Join(roots[role], filepath.FromSlash(relative))] = true
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(filename string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				for _, stage := range stages {
					if filename == stage {
						return filepath.SkipDir
					}
				}
				return nil
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("non-regular runtime restore target: %s", filename)
			}
			if allowed[filename] || filename == journal || filename == filepath.Join(roots["data"], ".graphdb.lock") {
				return nil
			}
			return fmt.Errorf("runtime restore requires empty targets: %s", filename)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func writeRuntimeRestorePlan(journal string, plan runtimeRestorePlan) error {
	data, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(journal), ".runtime-restore-plan-")
	if err != nil {
		return err
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), journal); err != nil {
		return err
	}
	return syncRuntimeDirectory(filepath.Dir(journal))
}
