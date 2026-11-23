package scaffold

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveRoundTripsAndIsDeterministic(t *testing.T) {
	files := []File{{Path: "k8s/base/deployment.yaml", Content: []byte("kind: Deployment\n")}, {Path: "Dockerfile", Content: []byte("FROM scratch\n")}}
	var a, b bytes.Buffer
	if err := WriteTarGz(&a, "payments-api", files); err != nil {
		t.Fatal(err)
	}
	if err := WriteTarGz(&b, "payments-api", []File{files[1], files[0]}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("the same files produced different archives")
	}
	got, err := ReadTarGz(bytes.NewReader(a.Bytes()), "payments-api")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Path != "Dockerfile" || string(got[1].Content) != "kind: Deployment\n" {
		t.Fatalf("round trip: %+v", got)
	}
}

func archive(t *testing.T, entries ...*tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, h := range entries {
		if h.Typeflag == tar.TypeReg && h.Size == 0 {
			h.Size = 1
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			_, _ = tw.Write(bytes.Repeat([]byte("x"), int(h.Size)))
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

func TestReadRejectsHostileArchives(t *testing.T) {
	cases := map[string][]byte{
		"traversal":      archive(t, &tar.Header{Typeflag: tar.TypeReg, Name: "payments-api/../../.bashrc", Mode: 0o644}),
		"other root":     archive(t, &tar.Header{Typeflag: tar.TypeReg, Name: "other/Dockerfile", Mode: 0o644}),
		"absolute":       archive(t, &tar.Header{Typeflag: tar.TypeReg, Name: "/etc/passwd", Mode: 0o644}),
		"symlink":        archive(t, &tar.Header{Typeflag: tar.TypeSymlink, Name: "payments-api/link", Linkname: "/etc"}),
		"oversize":       archive(t, &tar.Header{Typeflag: tar.TypeReg, Name: "payments-api/big", Size: MaxFileBytes + 1, Mode: 0o644}),
		"duplicate":      archive(t, &tar.Header{Typeflag: tar.TypeReg, Name: "payments-api/a", Mode: 0o644}, &tar.Header{Typeflag: tar.TypeReg, Name: "payments-api/a", Mode: 0o644}),
		"not an archive": []byte("<html>proxy error</html>"),
	}
	for name, raw := range cases {
		if _, err := ReadTarGz(bytes.NewReader(raw), "payments-api"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestWriteDirReplacesAtomically(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "services", "payments-api")
	if err := WriteDir(dir, []File{{Path: "stale.txt", Content: []byte("old")}, {Path: "Dockerfile", Content: []byte("v1")}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteDir(dir, []File{{Path: "Dockerfile", Content: []byte("v2")}}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "Dockerfile")); string(b) != "v2" {
		t.Fatalf("Dockerfile = %q", b)
	}
	if _, err := os.Stat(filepath.Join(dir, "stale.txt")); !os.IsNotExist(err) {
		t.Fatal("a file from the previous scaffold survived")
	}

	// A failure part-way leaves the previous scaffold and no partial tree.
	err := WriteDir(dir, []File{{Path: "ok.txt", Content: []byte("x")}, {Path: "../escape", Content: []byte("x")}})
	if err == nil {
		t.Fatal("a path leaving the scaffold was written")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "Dockerfile")); string(b) != "v2" {
		t.Fatalf("failed write changed the existing scaffold: %q", b)
	}
	entries, _ := os.ReadDir(filepath.Dir(dir))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") || strings.Contains(e.Name(), ".old-") {
			t.Fatalf("left behind %s", e.Name())
		}
	}
}
