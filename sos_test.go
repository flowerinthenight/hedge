package hedge

import (
	"os"
	"testing"
)

func TestSoS_BasicLocal(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	sos := op.NewSoS("test-sos", &SoSOptions{MemLimit: 1024, DiskLimit: 1024})

	w, err := sos.Writer(&writerOptions{LocalOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("hello "))
	w.Write([]byte("world"))
	w.Close()

	r, err := sos.Reader(&readerOptions{LocalOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []byte)
	go func() {
		r.Read(ch)
	}()

	var out []byte
	for b := range ch {
		out = append(out, b...)
	}
	r.Close()

	if string(out) != "hello world" {
		t.Errorf("expected 'hello world', got %s", string(out))
	}
}

func TestSoS_DiskSpillLocal(t *testing.T) {
	op := New(nil, "localhost:12345", "lock", "name", "log")
	sos := op.NewSoS("test-spill", &SoSOptions{MemLimit: 5, DiskLimit: 1024})

	w, err := sos.Writer(&writerOptions{LocalOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	
	part1 := []byte("first")
	part2 := []byte("second-part-goes-to-disk")
	w.Write(part1)
	w.Write(part2)
	w.Close()

	file := sos.localFile()
	if _, err := os.Stat(file); os.IsNotExist(err) {
		t.Errorf("expected local file %s to be created for spill", file)
	}

	r, err := sos.Reader(&readerOptions{LocalOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []byte)
	go func() {
		r.Read(ch)
	}()

	var out []byte
	for b := range ch {
		out = append(out, b...)
	}
	r.Close()

	expected := string(part1) + string(part2)
	if string(out) != expected {
		t.Errorf("expected %q, got %q", expected, string(out))
	}

	os.Remove(file)
}
