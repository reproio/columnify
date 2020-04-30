package columnifier

import (
	"reflect"
	"testing"

	"github.com/xitongsys/parquet-go/parquet"
)

func TestNewConfig(t *testing.T) {
	cases := []struct {
		parquetPageSize         int64
		parquetRowGroupSize     int64
		parquetCompressionCodec string
		expected                *Config
		isErr                   bool
	}{
		{
			parquetPageSize:         8 * 1024,
			parquetRowGroupSize:     128 * 1024 * 1024,
			parquetCompressionCodec: "SNAPPY",
			expected: &Config{
				Parquet: Parquet{
					PageSize:         8 * 1024,
					RowGroupSize:     128 * 1024 * 1024,
					CompressionCodec: parquet.CompressionCodec_SNAPPY,
				},
			},
			isErr: false,
		},

		{
			parquetPageSize:         8 * 1024,
			parquetRowGroupSize:     128 * 1024 * 1024,
			parquetCompressionCodec: "INVALID",
			expected:                nil,
			isErr:                   true,
		},
	}

	for _, c := range cases {
		actual, err := NewConfig(c.parquetPageSize, c.parquetRowGroupSize, c.parquetCompressionCodec)

		if err != nil != c.isErr {
			t.Errorf("expected %v, but actual %v", c.isErr, err)
		}

		if !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("expected %v, but actual %v", c.expected, actual)
		}
	}
}
