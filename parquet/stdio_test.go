package parquet

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	data := []byte("test")
	in := ioutil.NopCloser(bytes.NewBuffer(data))

	sf := stdioFile{
		in: in,
	}

	buf := make([]byte, len(data))
	if _, err := sf.Read(buf); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if bytes.Compare(data, buf) != 0 {
		t.Errorf("read data does not match written data: write %v, but read %v", data, buf)
	}
}

func TestWrite(t *testing.T) {
	f, err := ioutil.TempFile("", "stdio-write")
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

	written, err := ioutil.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(data, written) != 0 {
		t.Errorf("read data does not match written data: write %v, but read %v", data, written)
	}
}

func TestSeek(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Seek(1, 1)
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}

func TestOpen(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Open("stdio-open")
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}

func TestCreate(t *testing.T) {
	sf := stdioFile{}

	_, err := sf.Create("stdio-create")
	if !errors.Is(err, ErrUnsupportedMethod) {
		t.Errorf("expected: %v, but actual: %v\n", ErrUnsupportedMethod, err)
	}
}
