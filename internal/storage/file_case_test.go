package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectorySpellingCacheInvalidatesAndBounds(t *testing.T) {
	root := t.TempDir()
	files := NewFileStore(root)
	files.caseMode.Store(2)
	for _, part := range []string{"alpha", "sigma-Σ"} {
		if err := os.Mkdir(filepath.Join(root, part), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := files.checkDirectorySpelling(root, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := files.checkDirectorySpelling(root, "sigma-ς"); err == nil {
		t.Fatal("unicode case alias accepted")
	}
	if err := os.Mkdir(filepath.Join(root, "NEW"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := files.checkDirectorySpelling(root, "new"); err == nil {
		t.Fatal("cached listing missed new alias")
	}
	for i := range 270 {
		dir := filepath.Join(root, fmt.Sprint(i))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := files.checkDirectorySpelling(dir, "child"); err != nil {
			t.Fatal(err)
		}
	}
	if len(files.directorySpellings) > 256 || files.directorySpellingBytes > 8<<20 {
		t.Fatal("directory cache exceeded bounds")
	}
}
