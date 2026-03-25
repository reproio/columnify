package record

import (
	"bufio"
	"fmt"
	"io"
	"errors"
	"os"
	"strings"
	"strconv"

	"github.com/reproio/columnify/schema"
)

type tsvInnerDecoder struct {
	scanner *bufio.Scanner
	names   []string
}

var ErrTooFewFields = errors.New("too few fields")

func newTsvInnerDecoder(r io.Reader, s *schema.IntermediateSchema) (*tsvInnerDecoder, error) {
	if s == nil || r == nil {
		return nil, fmt.Errorf("invalid input: schema and reader must not be nil")
	}

	names, err := getFieldNamesFromSchema(s)
	if err != nil {
		return nil, err
	}
	
	return &tsvInnerDecoder{scanner: bufio.NewScanner(r), names: names}, nil
}

func (d *tsvInnerDecoder) Decode(r *map[string]interface{}) error {
	if !d.scanner.Scan() {
		if err := d.scanner.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	line := d.scanner.Text()
	fields := strings.Split(line, "\t")
	numNames := len(d.names)
	       if len(fields) < numNames {
		       return fmt.Errorf("%w: expected %d, got %d", ErrTooFewFields, numNames, len(fields))
	       }
	if len(fields) > numNames {
		fmt.Fprintf(os.Stderr, "warning: extra fields in TSV input, ignoring: %v\n", fields[numNames:])
	}
	record := make(map[string]interface{}, numNames)
	for i, n := range d.names {
		v := fields[i]
		// bool
		if v != "0" && v != "1" {
			if vv, err := strconv.ParseBool(v); err == nil {
				record[n] = vv
				continue
			}
		}
		// int
		if vv, err := strconv.ParseInt(v, 10, 64); err == nil {
			record[n] = vv
			continue
		}
		// float
		if vv, err := strconv.ParseFloat(v, 64); err == nil {
			record[n] = vv
			continue
		}
		// others; to string
		record[n] = v
	}
	*r = record
	return nil
}
