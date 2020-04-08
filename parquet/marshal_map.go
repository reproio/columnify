package parquet

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

// MarshalMap converts []map[string]interface{} to parquet tables.
func MarshalMap(sources []interface{}, bgn int, end int, schemaHandler *schema.SchemaHandler) (*map[string]*layout.Table, error) {
	res, err := prepareTables(schemaHandler)
	if err != nil {
		return nil, err
	}

	nodeBuf := marshal.NewNodeBuf(1)

	stack := make([]*marshal.Node, 0, 100)
	for _, d := range sources[bgn:end] {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = reflect.ValueOf(d)
		node.PathMap = schemaHandler.PathMap

		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			pathStr := node.PathMap.Path

			var info *common.Tag
			if i, ok := schemaHandler.MapIndex[pathStr]; ok {
				info = schemaHandler.Infos[i]
			} else {
				//no schema item will be ignored
				continue
			}

			switch node.Val.Type().Kind() {
			case reflect.Map:
				keys := node.Val.MapKeys()

				if info.Type == "MAP" { //real map
					pathStr = pathStr + ".Key_value"
					if len(keys) <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}

					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
					for j := len(keys) - 1; j >= 0; j-- {
						key := keys[j]
						value := node.Val.MapIndex(key).Elem()

						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
						newNode.Val = key
						newNode.DL = node.DL + 1
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)

						newNode = nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
						newNode.Val = value
						newNode.DL = node.DL + 1
						newPathStr := newNode.PathMap.Path // check again
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newInfo := schemaHandler.Infos[newSchemaIndex]
						if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL { //map value only be :optional or required
							newNode.DL++
						}

						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)
					}
				} else { //struct
					keysMap := make(map[string]int)
					for i, key := range keys {
						//ExName to InName
						keysMap[common.HeadToUpper(key.String())] = i
					}
					for key, _ := range node.PathMap.Children {
						ki, ok := keysMap[key]

						if ok && node.Val.MapIndex(keys[ki]).Elem().IsValid() { // non-null
							newNode := nodeBuf.GetNode()
							newNode.PathMap = node.PathMap.Children[key]
							newNode.Val = node.Val.MapIndex(keys[ki]).Elem()
							newNode.RL = node.RL
							newNode.DL = node.DL
							newPathStr := newNode.PathMap.Path
							newSchemaIndex := schemaHandler.MapIndex[newPathStr]
							newInfo := schemaHandler.Infos[newSchemaIndex]
							if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
								newNode.DL++
							}
							stack = append(stack, newNode)

						} else { // null
							newPathStr := node.PathMap.Children[key].Path
							for path, table := range res {
								if strings.HasPrefix(path, newPathStr) &&
									(len(path) == len(newPathStr) || path[len(newPathStr)] == '.') {

									table.Values = append(table.Values, nil)
									table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
									table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
								}
							}
						}
					}
				}

			case reflect.Slice:
				ln := node.Val.Len()

				if info.Type == "LIST" { //real LIST
					pathStr = pathStr + ".List" + ".Element"
					if ln <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["List"].Children["Element"]
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1

						newPathStr := newNode.PathMap.Path
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newInfo := schemaHandler.Infos[newSchemaIndex]
						if newInfo.RepetitionType == parquet.FieldRepetitionType_OPTIONAL { //element of LIST can only be optional or required
							newNode.DL++
						}

						stack = append(stack, newNode)
					}

				} else if info.Type == "BYTE_ARRAY" || info.Type == "FIXED_LEN_BYTE_ARRAY" { // byte array; its a primitive type
					v, err := marshalPrimitive(node.Val, info)
					if err != nil {
						return nil, err
					}

					table := res[node.PathMap.Path]
					table.Values = append(table.Values, v)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)

				} else { //Repeated
					if ln <= 0 {
						for key, table := range res {
							if len(key) >= len(node.PathMap.Path) &&
								key[:len(node.PathMap.Path)] == node.PathMap.Path {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1
						stack = append(stack, newNode)
					}
				}

			default: // else; should be primitive types
				v, err := marshalPrimitive(node.Val, info)
				if err != nil {
					return nil, err
				}

				table := res[node.PathMap.Path]
				table.Values = append(table.Values, v)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	}

	return &res, nil
}

func marshalPrimitive(val reflect.Value, info *common.Tag) (interface{}, error) {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil, fmt.Errorf("invalid input %v: %w", val.Type(), ErrInvalidParquetRecord)
	}

	pT, cT := types.TypeNameToParquetType(info.Type, info.BaseType)

	var s string
	if (*pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY) && cT == nil && val.Kind() == reflect.Slice { // raw binary
		var buf bytes.Buffer
		encoder := base64.NewEncoder(base64.StdEncoding, &buf)
		defer func() { _ = encoder.Close() }()

		if _, err := encoder.Write(val.Bytes()); err != nil {
			return nil, err
		}
		s = buf.String()
	} else {
		s = fmt.Sprintf("%v", val)
	}

	return types.StrToParquetType(s, pT, cT, int(info.Length), int(info.Scale)), nil
}
