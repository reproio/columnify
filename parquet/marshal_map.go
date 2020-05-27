package parquet

import (
	"encoding/json"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/schema"
)

// MarshalMap converts []map[string]interface{} to parquet tables.
func MarshalMap(sources []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	jsons := make([]interface{}, 0, end-bgn)

	for _, d := range sources[bgn:end] {
		e, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		jsons = append(jsons, string(e))
	}

	// NOTE: reuse existing JSON marshaler. Implementing it ourselves is high cost
	// NOTE: it requires redundant map -> json -> map conversions
	return marshal.MarshalJSON(jsons, bgn, end, schemaHandler)
}
