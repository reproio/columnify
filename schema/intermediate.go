package schema

import "github.com/apache/arrow/go/arrow"

type IntermediateSchema struct {
	ArrowSchema *arrow.Schema
	Name        string
}

func NewIntermediateSchema(s *arrow.Schema, name string) *IntermediateSchema {
	return &IntermediateSchema{
		ArrowSchema: s,
		Name:        name,
	}
}
