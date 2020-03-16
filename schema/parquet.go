package schema

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

var (
	arrowToParquet = map[arrow.DataType]string{
		arrow.FixedWidthTypes.Boolean: "BOOLEAN",
		arrow.PrimitiveTypes.Uint32:   "INT32",
		arrow.PrimitiveTypes.Uint64:   "INT64",
		arrow.PrimitiveTypes.Float32:  "FLOAT",
		arrow.PrimitiveTypes.Float64:  "DOUBLE",
		arrow.BinaryTypes.Binary:      "BYTE_ARRAY",
		arrow.BinaryTypes.String:      "UTF8",
	}
)

func NewSchemaHandlerFromArrow(s IntermediateSchema) (*schema.SchemaHandler, error) {
	elems := make([]*parquet.SchemaElement, 0)
	tags := make([]*common.Tag, 0)

	// record
	numChildren := int32(len(s.ArrowSchema.Fields()))
	rootElem := &parquet.SchemaElement{
		Name:           s.Name,
		NumChildren:    &numChildren,
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
	}
	rootTag := common.Tag{
		ExName: rootElem.GetName(),
		InName: common.HeadToUpper(rootElem.GetName()),
		Type:   "", // empty string indicates record type
	}
	elems = append(elems, rootElem)
	tags = append(tags, &rootTag)

	// fields under the record
	for _, child := range s.ArrowSchema.Fields() {
		e, tag, err := arrowFieldToParquetSchemaInfo(child)
		if err != nil {
			return nil, err
		}
		elems = append(elems, e...)
		tags = append(tags, tag...)
	}

	sh := schema.NewSchemaHandlerFromSchemaList(elems)

	// NOTE parquet-go erases tag info used to write files by NewSchemaHandlerFromSchemaList()
	// So rewrite it here, like json implementation in parquet-go
	sh.Infos = tags

	return sh, nil
}

func arrowFieldToParquetSchemaInfo(f arrow.Field) ([]*parquet.SchemaElement, []*common.Tag, error) {
	// primitive types
	if tn, ok := arrowToParquet[f.Type]; ok {
		t, ct := types.TypeNameToParquetType(tn, "")

		e := &parquet.SchemaElement{
			Type:           t,
			Name:           f.Name,
			ConvertedType:  ct,
			RepetitionType: arrowNullableToParquetRepetitionType(f.Nullable),
		}
		tag := &common.Tag{
			ExName: e.GetName(),
			InName: common.HeadToUpper(e.GetName()),
			Type:   tn,
		}

		return []*parquet.SchemaElement{e}, []*common.Tag{tag}, nil
	}

	// nested types
	if f.Type.ID() == arrow.STRUCT {
		if st, ok := f.Type.(*arrow.StructType); ok {
			elems := make([]*parquet.SchemaElement, 0)
			tags := make([]*common.Tag, 0)

			// record
			numChildren := int32(len(st.Fields()))
			rootElem := &parquet.SchemaElement{
				Name:           f.Name,
				NumChildren:    &numChildren,
				RepetitionType: arrowNullableToParquetRepetitionType(f.Nullable),
			}
			rootTag := common.Tag{
				ExName: rootElem.GetName(),
				InName: common.HeadToUpper(rootElem.GetName()),
				Type:   "", // empty string indicates record type
			}
			elems = append(elems, rootElem)
			tags = append(tags, &rootTag)

			// fields under the record
			for _, child := range st.Fields() {
				e, tag, err := arrowFieldToParquetSchemaInfo(child)
				if err != nil {
					return nil, nil, err
				}
				elems = append(elems, e...)
				tags = append(tags, tag...)
			}

			return elems, tags, nil
		}
	}

	// TODO other non-primitive types

	return nil, nil, fmt.Errorf("invalid schema conversion at %v", f)
}

func arrowNullableToParquetRepetitionType(nullable bool) *parquet.FieldRepetitionType {
	if nullable {
		return parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	} else {
		return parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	}
}
