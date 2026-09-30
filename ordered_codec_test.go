package extsort

import (
	"cmp"
	"context"
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"testing"
)

type (
	namedInt    int
	namedInt8   int8
	namedUint16 uint16
	namedFloat  float64
	namedString string
)

// roundTrip encodes and decodes each value with the Ordered codec for T and checks
// that the value and its exact encoding (including float bits) survive.
func roundTrip[T cmp.Ordered](t *testing.T, values ...T) {
	t.Helper()
	fromBytes, toBytes := orderedCodec[T]()
	for _, v := range values {
		d, err := toBytes(v)
		if err != nil {
			t.Fatalf("toBytes(%v): %v", v, err)
		}
		got, err := fromBytes(d)
		if err != nil {
			t.Fatalf("fromBytes(toBytes(%v)): %v", v, err)
		}
		if cmp.Compare(got, v) != 0 {
			t.Errorf("round trip of %v (%T) gave %v", v, v, got)
		}
		if again, _ := toBytes(got); string(again) != string(d) {
			t.Errorf("round trip of %v (%T) changed its encoding from %x to %x", v, v, d, again)
		}
	}
}

func TestOrderedCodecRoundTrip(t *testing.T) {
	negZero := math.Copysign(0, -1)
	t.Run("int", func(t *testing.T) { roundTrip(t, 0, 1, -1, math.MaxInt, math.MinInt) })
	t.Run("int8", func(t *testing.T) { roundTrip[int8](t, 0, -1, math.MaxInt8, math.MinInt8) })
	t.Run("int16", func(t *testing.T) { roundTrip[int16](t, 0, math.MaxInt16, math.MinInt16) })
	t.Run("int32", func(t *testing.T) { roundTrip[int32](t, 0, math.MaxInt32, math.MinInt32) })
	t.Run("int64", func(t *testing.T) { roundTrip[int64](t, 0, math.MaxInt64, math.MinInt64) })
	t.Run("uint", func(t *testing.T) { roundTrip[uint](t, 0, 1, math.MaxUint) })
	t.Run("uint8", func(t *testing.T) { roundTrip[uint8](t, 0, math.MaxUint8) })
	t.Run("uint16", func(t *testing.T) { roundTrip[uint16](t, 0, math.MaxUint16) })
	t.Run("uint32", func(t *testing.T) { roundTrip[uint32](t, 0, math.MaxUint32) })
	t.Run("uint64", func(t *testing.T) { roundTrip[uint64](t, 0, math.MaxUint64) })
	t.Run("uintptr", func(t *testing.T) { roundTrip[uintptr](t, 0, ^uintptr(0)) })
	t.Run("float32", func(t *testing.T) {
		roundTrip[float32](t, 0, float32(negZero), 1.5, -2.25, math.MaxFloat32, math.SmallestNonzeroFloat32,
			float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()))
	})
	t.Run("float64", func(t *testing.T) {
		roundTrip(t, 0, negZero, 1.5, -2.25, math.MaxFloat64, math.SmallestNonzeroFloat64,
			math.Inf(1), math.Inf(-1), math.NaN())
	})
	t.Run("string", func(t *testing.T) {
		roundTrip(t, "", "a", "héllo, 世界", "\x00\xff not UTF-8", strings.Repeat("x", 1<<16))
	})
	t.Run("named types", func(t *testing.T) {
		roundTrip[namedInt](t, -42, math.MaxInt, math.MinInt)
		roundTrip[namedInt8](t, math.MinInt8, math.MaxInt8)
		roundTrip[namedUint16](t, 0, math.MaxUint16)
		roundTrip(t, namedFloat(negZero), namedFloat(math.NaN()), namedFloat(math.Inf(-1)))
		roundTrip[namedString](t, "", "named")
	})
}

// decodeErr adapts a FromBytesGeneric to return only its error.
func decodeErr[T any](fromBytes FromBytesGeneric[T]) func([]byte) error {
	return func(d []byte) error {
		_, err := fromBytes(d)
		return err
	}
}

func TestOrderedCodecRejectsInvalidData(t *testing.T) {
	intFromBytes, _ := orderedCodec[int]()
	int8FromBytes, _ := orderedCodec[int8]()
	uint8FromBytes, _ := orderedCodec[uint8]()
	float32FromBytes, _ := orderedCodec[float32]()
	float64FromBytes, _ := orderedCodec[float64]()
	for _, tc := range []struct {
		name   string
		decode func([]byte) error
		data   []byte
	}{
		{"int: empty", decodeErr(intFromBytes), nil},
		{"int: truncated varint", decodeErr(intFromBytes), []byte{0x80}},
		{"int: trailing bytes", decodeErr(intFromBytes), []byte{0x02, 0x00}},
		{"int8: out of range", decodeErr(int8FromBytes), binary.AppendVarint(nil, 300)},
		{"uint8: out of range", decodeErr(uint8FromBytes), binary.AppendUvarint(nil, 256)},
		{"float32: wrong length", decodeErr(float32FromBytes), []byte{1, 2, 3}},
		{"float64: wrong length", decodeErr(float64FromBytes), []byte{1, 2, 3, 4}},
	} {
		if err := tc.decode(tc.data); err == nil {
			t.Errorf("%s: decoding %x succeeded, want an error", tc.name, tc.data)
		}
	}
}

// checkOrderedSort sorts values with Ordered through temp files on disk and compares
// the output with slices.SortFunc and cmp.Compare.
func checkOrderedSort[T cmp.Ordered](t *testing.T, values []T) {
	t.Helper()
	in := make(chan T, len(values))
	for _, v := range values {
		in <- v
	}
	close(in)
	sorter, out, errc := Ordered(in, &Config{ChunkSize: 2, TempFilesDir: t.TempDir()})
	sorter.Sort(context.Background())
	got, err := drainWithTimeout(t, out, errc)
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(values)
	slices.SortFunc(want, cmp.Compare[T])
	if slices.CompareFunc(got, want, cmp.Compare[T]) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Ordered used gob, which handled these types but allocated a new encoder and decoder
// per record. The binary codec must sort them the same way.
func TestOrderedSortsThroughDisk(t *testing.T) {
	t.Run("float64 with NaN and -0", func(t *testing.T) {
		checkOrderedSort(t, []float64{3, math.NaN(), -1, math.Inf(1), math.Copysign(0, -1), 0, math.Inf(-1), math.NaN(), 2.5})
	})
	t.Run("float32", func(t *testing.T) {
		checkOrderedSort(t, []float32{2.5, -1, float32(math.NaN()), 0, 1e30})
	})
	t.Run("named int", func(t *testing.T) {
		checkOrderedSort(t, []namedInt{5, -3, math.MaxInt, math.MinInt, 0, 5})
	})
	t.Run("uint8", func(t *testing.T) {
		checkOrderedSort(t, []uint8{255, 0, 7, 128, 7})
	})
	t.Run("named string", func(t *testing.T) {
		checkOrderedSort(t, []namedString{"pear", "", "apple", "fig", "apple", "\xff"})
	})
}
