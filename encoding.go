package main

import (
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

func encodeTaggedValue(value interface{}) []byte {

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
	return taggedV
}
