package record

import (
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/linkedin/goavro/v2"
	"github.com/reproio/columnify/schema"
)

type avroInnerDecoder struct {
	r      *goavro.OCFReader
	schema *schema.IntermediateSchema
}

func newAvroInnerDecoder(r io.Reader, s *schema.IntermediateSchema) (*avroInnerDecoder, error) {
	reader, err := goavro.NewOCFReader(r)
	if err != nil {
		return nil, err
	}

	return &avroInnerDecoder{
		r:      reader,
		schema: s,
	}, nil
}

func (d *avroInnerDecoder) Decode(r *map[string]interface{}) error {
	if d.r.Scan() {
		v, err := d.r.Read()
		if err != nil {
			return err
		}

		m, mapOk := v.(map[string]interface{})
		if !mapOk {
			return fmt.Errorf("invalid value %v: %w", v, ErrUnconvertibleRecord)
		}

		flatten := flattenAvroUnion(m)
		*r = convertAvroLogicalValues(flatten, d.schema.ArrowSchema.Fields())
	} else if d.r.RemainingBlockItems() == 0 {
		if d.r.Err() != nil {
			return d.r.Err()
		}
		return io.EOF
	}

	return d.r.Err()
}

// flattenAvroUnion flattens nested map type has only 1 element.
func flattenAvroUnion(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})

	for k, v := range in {
		if m, ok := v.(map[string]interface{}); ok {
			// Flatten because Avro-JSON representation has redundant nested map type.
			// see also https://github.com/linkedin/goavro#translating-from-go-to-avro-data
			if len(m) == 1 {
				for _, vv := range m {
					out[k] = vv
					break
				}
			} else {
				out[k] = flattenAvroUnion(m)
			}
		} else {
			out[k] = v
		}
	}

	return out
}

// convertAvroLogicalValues converts values decoded from Avro logical types (time.Time and time.Duration)
// into integers in the unit of the corresponding arrow type, which parquet columns expect.
func convertAvroLogicalValues(in map[string]interface{}, fields []arrow.Field) map[string]interface{} {
	for _, f := range fields {
		if v, ok := in[f.Name]; ok {
			in[f.Name] = convertAvroLogicalValue(v, f.Type)
		}
	}

	return in
}

func convertAvroLogicalValue(v interface{}, t arrow.DataType) interface{} {
	switch vv := v.(type) {
	case time.Time:
		switch t {
		case arrow.FixedWidthTypes.Date32:
			const secondsPerDay = int64(24 * time.Hour / time.Second)
			days := vv.Unix() / secondsPerDay
			if vv.Unix()%secondsPerDay < 0 {
				days--
			}
			return int32(days)
		case arrow.FixedWidthTypes.Timestamp_ms:
			return vv.UnixMilli()
		case arrow.FixedWidthTypes.Timestamp_us:
			return vv.UnixMicro()
		}
	case time.Duration:
		switch t {
		case arrow.FixedWidthTypes.Time32ms:
			return int32(vv.Milliseconds())
		case arrow.FixedWidthTypes.Time64us:
			return vv.Microseconds()
		}
	case map[string]interface{}:
		if st, ok := t.(*arrow.StructType); ok {
			return convertAvroLogicalValues(vv, st.Fields())
		}
	case []interface{}:
		if lt, ok := t.(*arrow.ListType); ok {
			for i := range vv {
				vv[i] = convertAvroLogicalValue(vv[i], lt.Elem())
			}
		}
	}

	return v
}
