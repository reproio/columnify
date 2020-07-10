package json

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

func Example_writer() {
	f := new(bytes.Buffer)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2", "str-3", "str-4", "str-5", "str-6", "str-7", "str-8", "str-9"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	w := NewWriter(f, schema)
	err := w.Write(rec)
	if err != nil {
		log.Fatal(err)
	}
}

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
{"bool":true,"f32":0,"f64":0,"i16":-1,"i32":-1,"i64":-1,"i8":-1,"str":"str-0","u16":0,"u32":0,"u64":0,"u8":0},
{"bool":false,"f32":0.1,"f64":0.1,"i16":0,"i32":0,"i64":0,"i8":0,"str":"str-1","u16":1,"u32":1,"u64":1,"u8":1},
{"bool":true,"f32":0.2,"f64":0.2,"i16":1,"i32":1,"i64":1,"i8":1,"str":"str-2","u16":2,"u32":2,"u64":2,"u8":2},
{"bool":null,"f32":null,"f64":null,"i16":null,"i32":null,"i64":null,"i8":null,"str":null,"u16":null,"u32":null,"u64":null,"u8":null}]
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
