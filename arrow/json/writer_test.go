package json

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestJsonWriter(t *testing.T) {
	tests := []struct {
		name string
	}{{
		name: "Primitives",
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testJsonWriter(t)
		})
	}
}

func testJsonWriter(t *testing.T) {
	f := new(bytes.Buffer)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "bin", Type: arrow.BinaryTypes.Binary},
			{Name: "struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
				{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
				{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
				{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
				{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
				{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
				{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
				{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
				{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
				{Name: "str", Type: arrow.BinaryTypes.String},
				{Name: "bin", Type: arrow.BinaryTypes.Binary},
			}...)},
			// TODO
			// {Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64)},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	b.Field(1).(*array.Int8Builder).AppendValues([]int8{-1, 0, 1}, nil)
	b.Field(2).(*array.Int16Builder).AppendValues([]int16{-1, 0, 1}, nil)
	b.Field(3).(*array.Int32Builder).AppendValues([]int32{-1, 0, 1}, nil)
	b.Field(4).(*array.Int64Builder).AppendValues([]int64{-1, 0, 1}, nil)
	b.Field(5).(*array.Uint8Builder).AppendValues([]uint8{0, 1, 2}, nil)
	b.Field(6).(*array.Uint16Builder).AppendValues([]uint16{0, 1, 2}, nil)
	b.Field(7).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
	b.Field(8).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2}, nil)
	b.Field(9).(*array.Float32Builder).AppendValues([]float32{0.0, 0.1, 0.2}, nil)
	b.Field(10).(*array.Float64Builder).AppendValues([]float64{0.0, 0.1, 0.2}, nil)
	b.Field(11).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2"}, nil)
	b.Field(12).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bin-0"), []byte("bin-1"), []byte("bin-2")}, nil)
	sb := b.Field(13).(*array.StructBuilder)
	sb.AppendValues([]bool{true, true, true})
	sb.FieldBuilder(0).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	sb.FieldBuilder(1).(*array.Int8Builder).AppendValues([]int8{-1, 0, 1}, nil)
	sb.FieldBuilder(2).(*array.Int16Builder).AppendValues([]int16{-1, 0, 1}, nil)
	sb.FieldBuilder(3).(*array.Int32Builder).AppendValues([]int32{-1, 0, 1}, nil)
	sb.FieldBuilder(4).(*array.Int64Builder).AppendValues([]int64{-1, 0, 1}, nil)
	sb.FieldBuilder(5).(*array.Uint8Builder).AppendValues([]uint8{0, 1, 2}, nil)
	sb.FieldBuilder(6).(*array.Uint16Builder).AppendValues([]uint16{0, 1, 2}, nil)
	sb.FieldBuilder(7).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
	sb.FieldBuilder(8).(*array.Uint64Builder).AppendValues([]uint64{0, 1, 2}, nil)
	sb.FieldBuilder(9).(*array.Float32Builder).AppendValues([]float32{0.0, 0.1, 0.2}, nil)
	sb.FieldBuilder(10).(*array.Float64Builder).AppendValues([]float64{0.0, 0.1, 0.2}, nil)
	sb.FieldBuilder(11).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2"}, nil)
	sb.FieldBuilder(12).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("bin-0"), []byte("bin-1"), []byte("bin-2")}, nil)
	/* TODO
	lb := b.Field(14).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.Uint64Builder).AppendValues([]uint64{0, 0, 0}, nil)
	lb.Append(true)
	lb.ValueBuilder().(*array.Uint64Builder).AppendValues([]uint64{1, 11, 111}, nil)
	lb.Append(true)
	lb.ValueBuilder().(*array.Uint64Builder).AppendValues([]uint64{2, 22, 22}, nil)
	*/

	for _, field := range b.Fields() {
		field.AppendNull()
	}

	rec := b.NewRecord()
	defer rec.Release()

	w := NewWriter(f, schema)
	err := w.Write(rec)
	if err != nil {
		t.Fatal(err)
	}

	want := strings.ReplaceAll(`[
{"bin":"YmluLTA=","bool":true,"f32":0,"f64":0,"i16":-1,"i32":-1,"i64":-1,"i8":-1,"str":"str-0","struct":{"bin":"YmluLTA=","bool":true,"f32":0,"f64":0,"i16":-1,"i32":-1,"i64":-1,"i8":-1,"str":"str-0","u16":0,"u32":0,"u64":0,"u8":0},"u16":0,"u32":0,"u64":0,"u8":0},
{"bin":"YmluLTE=","bool":false,"f32":0.1,"f64":0.1,"i16":0,"i32":0,"i64":0,"i8":0,"str":"str-1","struct":{"bin":"YmluLTE=","bool":false,"f32":0.1,"f64":0.1,"i16":0,"i32":0,"i64":0,"i8":0,"str":"str-1","u16":1,"u32":1,"u64":1,"u8":1},"u16":1,"u32":1,"u64":1,"u8":1},
{"bin":"YmluLTI=","bool":true,"f32":0.2,"f64":0.2,"i16":1,"i32":1,"i64":1,"i8":1,"str":"str-2","struct":{"bin":"YmluLTI=","bool":true,"f32":0.2,"f64":0.2,"i16":1,"i32":1,"i64":1,"i8":1,"str":"str-2","u16":2,"u32":2,"u64":2,"u8":2},"u16":2,"u32":2,"u64":2,"u8":2},
{"bin":null,"bool":null,"f32":null,"f64":null,"i16":null,"i32":null,"i64":null,"i8":null,"str":null,"struct":null,"u16":null,"u32":null,"u64":null,"u8":null}]
`, "\n", "") + "\n"

	if got, want := f.String(), want; strings.Compare(got, want) != 0 {
		t.Fatalf("invalid output:\ngot=%s\nwant=%s\n", got, want)
	}
}

func BenchmarkWrite(b *testing.B) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(b, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "bin", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	const N = 1000
	for i := 0; i < N; i++ {
		bldr.Field(0).(*array.BooleanBuilder).Append(i%10 == 0)
		bldr.Field(1).(*array.Int8Builder).Append(int8(i))
		bldr.Field(2).(*array.Int16Builder).Append(int16(i))
		bldr.Field(3).(*array.Int32Builder).Append(int32(i))
		bldr.Field(4).(*array.Int64Builder).Append(int64(i))
		bldr.Field(5).(*array.Uint8Builder).Append(uint8(i))
		bldr.Field(6).(*array.Uint16Builder).Append(uint16(i))
		bldr.Field(7).(*array.Uint32Builder).Append(uint32(i))
		bldr.Field(8).(*array.Uint64Builder).Append(uint64(i))
		bldr.Field(9).(*array.Float32Builder).Append(float32(i))
		bldr.Field(10).(*array.Float64Builder).Append(float64(i))
		bldr.Field(11).(*array.StringBuilder).Append(fmt.Sprintf("str-%d", i))
		bldr.Field(12).(*array.BinaryBuilder).Append([]byte(fmt.Sprintf("bin-%d", i)))
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	w := NewWriter(ioutil.Discard, schema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := w.Write(rec)
		if err != nil {
			b.Fatal(err)
		}
	}
}
