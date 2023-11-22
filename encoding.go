package main

import (
	"encoding/binary"
	"log"
	"math"
)

// makePVS returns a path value for a string value
func makePVS(path, value string) []byte {
	pv := []byte(path)
	pv = append(pv, 0)
	pv = append(pv, JSONTagString)
	pv = append(pv, []byte(value)...)
	return pv
}

// makePVB returns a path value for a boolean value
func makePVB(path string, value bool) []byte {
	pv := []byte(path)
	pv = append(pv, 0)
	if value {
		pv = append(pv, JSONTagTrue)
	} else {
		pv = append(pv, JSONTagFalse)
	}
	return pv
}

// makePVN returns a path value for a null value
func makePVN(path string) []byte {
	pv := []byte(path)
	pv = append(pv, 0)
	pv = append(pv, JSONTagNull)
	return pv
}

// makePVI returns a path value for an int value
func makePVI(path string, value float64) []byte {
	// This StackOverflow answer shows how to
	// encode a float64 into a byte array that
	// has the same sort order as the floats.
	// https://stackoverflow.com/a/54557561
	var buf [8]byte
	bits := math.Float64bits(value)
	if value >= 0 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xffffffffffffffff
	}
	binary.BigEndian.PutUint64(buf[:], bits)

	pv := []byte(path)
	pv = append(pv, 0)
	pv = append(pv, JSONTagNumber)
	pv = append(pv, buf[:]...)
	return pv
}

// pathValueAsKey returns a []byte key for path and value.
func pathValueAsKey(path string, value interface{}) []byte {
	// fmt.Printf("path: %+v, value: %+v\n", path, value)

	switch t := value.(type) {
	case nil:
		return makePVN(path)
	case bool:
		return makePVB(path, t)
	case float64:
		return makePVI(path, float64(t))
	case float32:
		return makePVI(path, float64(t))
	case uint:
		return makePVI(path, float64(t))
	case uint8:
		return makePVI(path, float64(t))
	case uint16:
		return makePVI(path, float64(t))
	case uint32:
		return makePVI(path, float64(t))
	case uint64:
		return makePVI(path, float64(t))
	case int:
		return makePVI(path, float64(t))
	case int8:
		return makePVI(path, float64(t))
	case int16:
		return makePVI(path, float64(t))
	case int32:
		return makePVI(path, float64(t))
	case int64:
		return makePVI(path, float64(t))
	case string:
		return makePVS(path, value.(string))
	default:
		// This should never happen from value JSON
		log.Printf("Unexpected type in pathValueAsKey: %+v\n", value)
		panic(1)
	}
}
