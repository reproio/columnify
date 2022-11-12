package parquet

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

type dummyStdioFile struct {
	readFunc  func(p []byte) (n int, err error)
	writeFunc func(p []byte) (n int, err error)
	closeFunc func() error
}

func (d dummyStdioFile) Read(p []byte) (n int, err error) {
	return d.readFunc(p)
}

func (d dummyStdioFile) Write(p []byte) (n int, err error) {
	return d.writeFunc(p)
}

func (d dummyStdioFile) Close() error {
	return d.closeFunc()
}

func TestNewStdioFile(t *testing.T) {
	f := NewStdioFile()

	if f == nil {
		t.Error("StdioFile should always be not nil")
	}
}

func TestStdioFileRead(t *testing.T) {
	data := []byte("test")
	in := io.NopCloser(bytes.NewBuffer(data))

	sf := stdioFile{
		in: in,
	}

	buf := make([]byte, len(data))
	if _, err := sf.Read(buf); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if !bytes.Equal(data, buf) {
		t.Errorf("read data does not match written data: write %v, but read %v", data, buf)
	}
}

func TestStdioFileWrite(t *testing.T) {
	f, err := os.CreateTemp("", "stdio-write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	sf := stdioFile{
		out: f,
	}

	data := []byte("test")
	if _, err := sf.Write(data); err != nil {
		t.Fatal(err)
	}

	written, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, written) {
		t.Errorf("read data does not match written data: write %v, but read %v", data, written)
	}
}

func TestStdioFileClose(t *testing.T) {
	inClosed := false
	in := dummyStdioFile{
		closeFunc: func() error {
			inClosed = true
			return nil
		},
	}

	outClosed := false
	out := dummyStdioFile{
		closeFunc: func() error {
			outClosed = true
			return nil
		},
	}

	f := &stdioFile{
		in:  in,
		out: out,
	}

	err := f.Close()
	if err != nil {
		t.Error(err)
	}

	if !inClosed || !outClosed {
		t.Error("expected to be called Close(), but actually it's not")
	}
}

func TestStdioFileSeek(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Seek(1, 1)
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}

func TestStdioFileOpen(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Open("stdio-open")
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}

func TestStdioFileCreate(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Create("stdio-create")
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}
