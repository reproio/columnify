package avro

func toPrimitiveType(v string) *primitiveType {
	vv := primitiveType(v)
	return &vv
}

func toDefinedType(v string) *definedType {
	vv := definedType(v)
	return &vv
}
