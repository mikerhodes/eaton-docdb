package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
)

var sep []byte = []byte{0}

func encodeFloat(value float64) []byte {
	// This StackOverflow answer shows how to
	// encode a float64 into a byte array that
	// has the same sort order as the floats.
	// https://stackoverflow.com/a/54557561
	buf := make([]byte, 8)
	bits := math.Float64bits(value)
	if value >= 0 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xffffffffffffffff
	}
	binary.BigEndian.PutUint64(buf[:], bits)
	return buf
}

// pathValueAsKey returns a []byte key for path and value.
func pathValueAsKey(path string, value interface{}) []byte {
	// fmt.Printf("path: %+v, value: %+v\n", path, value)

	// A key for path and value looks like:
	// [66, 6f, 6f, 00, 2c, 68, 65, 6c, 6c, 6f]
	//  ----------  --  --  ------------------
	//  path (foo)  |   |   value (hello)
	//              |   `JSONTagString
	//              `null between path and value

	// First, create the tagged value byte array representation
	var taggedV []byte

	// Helper to create a byte encoding of v including the type tag
	taggedF := func(v float64) []byte {
		return append([]byte{JSONTagNumber}, encodeFloat(v)...)
	}

	switch t := value.(type) {
	case nil:
		taggedV = []byte{JSONTagNull}
	case bool:
		if t {
			taggedV = []byte{JSONTagTrue}
		} else {
			taggedV = []byte{JSONTagFalse}
		}
	case float64:
		taggedV = taggedF(float64(t))
	case float32:
		taggedV = taggedF(float64(t))
	case uint:
		taggedV = taggedF(float64(t))
	case uint8:
		taggedV = taggedF(float64(t))
	case uint16:
		taggedV = taggedF(float64(t))
	case uint32:
		taggedV = taggedF(float64(t))
	case uint64:
		taggedV = taggedF(float64(t))
	case int:
		taggedV = taggedF(float64(t))
	case int8:
		taggedV = taggedF(float64(t))
	case int16:
		taggedV = taggedF(float64(t))
	case int32:
		taggedV = taggedF(float64(t))
	case int64:
		taggedV = taggedF(float64(t))
	case string:
		taggedS := append([]byte{JSONTagString}, []byte(value.(string))...)
		taggedV = taggedS
	default:
		// This should never happen from value JSON
		log.Printf("Unexpected type in pathValueAsKey: %+v\n", value)
		panic(1)
	}

	return packTuple([]byte(path), taggedV)
}

// packTuple packs a set of components into a packed byte array
// representation. Use unpackTuple[N] to unpack.
// 0x00 is used as the separator.
func packTuple(components ...[]byte) []byte {
	return bytes.Join(components, sep)
}

// unpackTuple unpacked packed into its components.
// It is equivalent to unpackTupleN with a count of -1.
func unpackTuple(packed []byte) [][]byte {
	return unpackTupleN(packed, -1)
}

// unpackTupleN unpacks packed into its components.
//
// The count determines the number of components to return:
//
//	n > 0: at most n subslices; the last subslice will be the unsplit remainder.
//	n == 0: the result is nil (zero subslices)
//	n < 0: all subslices
func unpackTupleN(packed []byte, n int) [][]byte {
	return bytes.SplitN(packed, sep, n)
}
