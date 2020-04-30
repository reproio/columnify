package columnifier

import (
	"github.com/xitongsys/parquet-go/parquet"
)

type Config struct {
	Parquet Parquet
}

type Parquet struct {
	PageSize         int64
	RowGroupSize     int64
	CompressionCodec parquet.CompressionCodec
}

func NewConfig(parquetPageSize, parquetRowGroupSize int64, parquetCompressionCodec string) (*Config, error) {
	cc, err := parquet.CompressionCodecFromString(parquetCompressionCodec)
	if err != nil {
		return nil, err
	}

	return &Config{
		Parquet: Parquet{
			PageSize:         parquetPageSize,
			RowGroupSize:     parquetRowGroupSize,
			CompressionCodec: cc,
		},
	}, nil
}
