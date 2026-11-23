// Package scaffold holds a rendered service scaffold in memory and moves it
// as a gzipped tar archive: the API renders and serves it without touching
// its own disk, and the CLI extracts it on the developer's machine.
package scaffold

import (
	"archive/tar"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// File is one file of a scaffold. Path is slash-separated and relative to
// the scaffold's root.
type File struct {
	Path    string
	Content []byte
}

// Limits on an archive being extracted, so a hostile or broken server
// cannot fill the disk.
const (
	MaxFiles     = 2000
	MaxFileBytes = 4 << 20
	MaxBytes     = 32 << 20
)

// archiveTime is the modification time of every entry, so the same
// scaffold always produces the same archive.
var archiveTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// CheckPath rejects a path that is absolute, empty, not clean, or leaves
// the scaffold's root.
func CheckPath(p string) error {
	if p == "" || p == "." || strings.HasPrefix(p, "/") || strings.Contains(p, `\`) ||
		path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") {
		return fmt.Errorf("scaffold path %q is not a clean relative path", p)
	}
	return nil
}

// WriteTarGz writes files as a gzipped tar with every entry under root/.
// Entries are sorted and carry fixed ownership and times.
func WriteTarGz(w io.Writer, root string, files []File) error {
	if err := CheckPath(root); err != nil || strings.Contains(root, "/") {
		return fmt.Errorf("archive root %q must be a single path element", root)
	}
	sorted := append([]File(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	for _, f := range sorted {
		if err := CheckPath(f.Path); err != nil {
			return err
		}
		hdr := &tar.Header{
			Typeflag: tar.TypeReg,
			Name:     root + "/" + f.Path,
			Mode:     0o644,
			Size:     int64(len(f.Content)),
			ModTime:  archiveTime,
			Format:   tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(f.Content); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// ReadTarGz reads an archive written by WriteTarGz. Every entry must be a
// regular file or directory under root/; anything else (links, devices,
// paths escaping root) and archives over the limits are rejected.
func ReadTarGz(r io.Reader, root string) ([]File, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("scaffold archive: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var (
		files []File
		total int64
		seen  = map[string]bool{}
	)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("scaffold archive: %w", err)
		}
		name := strings.TrimSuffix(hdr.Name, "/")
		rel, ok := strings.CutPrefix(name, root+"/")
		if name == root && hdr.Typeflag == tar.TypeDir {
			continue
		}
		if !ok {
			return nil, fmt.Errorf("scaffold archive entry %q is outside %s/", hdr.Name, root)
		}
		if err := CheckPath(rel); err != nil {
			return nil, err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
		default:
			return nil, fmt.Errorf("scaffold archive entry %q is not a regular file", hdr.Name)
		}
		if seen[rel] {
			return nil, fmt.Errorf("scaffold archive has %q twice", rel)
		}
		seen[rel] = true
		if len(files) >= MaxFiles {
			return nil, fmt.Errorf("scaffold archive has more than %d files", MaxFiles)
		}
		if hdr.Size > MaxFileBytes || total+hdr.Size > MaxBytes {
			return nil, fmt.Errorf("scaffold archive is over the size limit at %q", rel)
		}
		content, err := io.ReadAll(io.LimitReader(tr, hdr.Size))
		if err != nil {
			return nil, fmt.Errorf("scaffold archive %q: %w", rel, err)
		}
		total += int64(len(content))
		files = append(files, File{Path: rel, Content: content})
	}
	return files, nil
}

// WriteDir writes files to dir so that dir afterwards holds exactly those
// files, or, on any error, whatever it held before. The files are written
// to a temporary sibling directory that is renamed into place; an existing
// dir is moved aside first and removed once the new one is in place.
func WriteDir(dir string, files []File) (err error) {
	dir = filepath.Clean(dir)
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, "."+filepath.Base(dir)+".tmp-")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := os.Chmod(tmp, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		if err := CheckPath(f.Path); err != nil {
			return err
		}
		target := filepath.Join(tmp, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := writeFile(target, f.Content); err != nil {
			return err
		}
	}

	if _, statErr := os.Lstat(dir); errors.Is(statErr, os.ErrNotExist) {
		return os.Rename(tmp, dir)
	} else if statErr != nil {
		return statErr
	}
	old := dir + ".old-" + suffix()
	if err := os.Rename(dir, old); err != nil {
		return err
	}
	if err := os.Rename(tmp, dir); err != nil {
		if restoreErr := os.Rename(old, dir); restoreErr != nil {
			return fmt.Errorf("%w; the previous scaffold is at %s", err, old)
		}
		return err
	}
	return os.RemoveAll(old)
}

func writeFile(target string, content []byte) error {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return err
	}
	// A failed close can mean the data never reached the disk.
	return f.Close()
}

func suffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
