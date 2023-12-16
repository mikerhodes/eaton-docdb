package main

import (
	"bytes"
	"errors"
	"fmt"
)

// Tags for the values of JSON
// primitives. Objects and arrays
// are encoded into the path.
// Tags are inserted prior to the
// values in pathValues for keys to
// ensure sort ordering.
const (
	// printable character makes easier debugging
	JSONTagNull   = 0x28 // char: (
	JSONTagFalse  = 0x29 // char: )
	JSONTagTrue   = 0x2a // char: *
	JSONTagNumber = 0x2b // char: +
	JSONTagString = 0x2c // char: ,

)

type fwdIdxKey struct {
	id          []byte
	path        []byte
	taggedValue []byte
}

// decodeFwdIdxKey deserialises a fwdIndexKey from b
func decodeFwdIdxKey(b []byte) fwdIdxKey {
	// We need to use unpackTupleN because pathValueKey is
	// itself a tuple with multiple components, and we don't
	// want to split that.
	// [ fwdIdxNS, docId, pathValueKey ]
	parts := unpackTupleN(b, 4)
	return fwdIdxKey{
		id:          parts[1],
		path:        parts[2],
		taggedValue: parts[3],
	}
}

// encodeFwdIdxKey serialises k to a byte slice.
func encodeFwdIdxKey(k fwdIdxKey) []byte {
	return packTuple([]byte{fwdIdxNamespace}, k.id, k.path, k.taggedValue)
}

// encodeInvIdxKey returns a byte slice containing an inverted
// index key. Supplying nil path is invalid. Supplying nil
// taggedValue and/or docID results in a truncated key,
// truncated immediately after the last non-nil argument.
// The truncation behaviour allows this method to be used to
// generate path start/end keys during querying.
func encodeInvIdxKey(path, taggedValue, docID []byte) []byte {
	// A key for path and value looks like:
	// ['i', 00, 66, 6f, 6f, 00, 2c, 68, 65, 6c, 6c, 6f 00 docID]
	//   |       ----------  --  --  ------------------
	//   |       path (foo)  |   |   value (hello)
	//   |                   |   `JSONTagString
	//   `invIdxNamespace    `null between path and value
	var sep byte = 0
	buf := make([]byte, 0, 4+len(path)+len(taggedValue)+len(docID))
	buf = append(buf, []byte{invIdxNamespace, sep}...)
	buf = append(buf, path...)
	if taggedValue != nil {
		buf = append(buf, sep)
		buf = append(buf, taggedValue...)
		if docID != nil {
			buf = append(buf, sep)
			buf = append(buf, docID...)
		}
	}
	return buf
}

// Return type for decodeInvIdxKey
type InvIndexKey struct {
	Path, TaggedValue, DocID []byte
}

// unpack the key. nb returns value with tag.
func decodeInvIndexKey(k []byte) (InvIndexKey, error) {
	// [ type 00 path 00 tag value 00 docid ]
	// - value can contain 00 bytes --- particularly numbers
	// - null, true, false just have tag
	// our main issue is splitting up the [tag value 00 docid]
	// because we have to alter strategy depending on type
	iik := InvIndexKey{}
	sep := []byte{0}

	// Check namespace of key and following sep
	if k[0] != invIdxNamespace || k[1] != 0 {
		// error
		return iik, errors.New(
			fmt.Sprintf(
				"Invalid namespace for inverted index key (%d %d)", k[0], k[1],
			),
		)
	}
	k = k[2:]

	// path
	m := bytes.Index(k, sep)
	if m < 0 {
		return iik, errors.New("No path found in inverted index key")
	}
	iik.Path = k[:m:m]
	k = k[m+len(sep):]

	// value
	switch k[0] {
	case JSONTagNull:
		fallthrough
	case JSONTagTrue:
		fallthrough
	case JSONTagFalse:
		iik.TaggedValue = []byte{k[0]}
		k = k[1+len(sep):]
	case JSONTagNumber:
		n := 9 // tag + 8 byte float encoding
		iik.TaggedValue = k[0:n]
		k = k[n+len(sep):]
	case JSONTagString:
		m = bytes.Index(k, sep)
		if m < 0 {
			return iik, errors.New(
				"String type with no value found in inverted index key")
		}
		iik.TaggedValue = k[:m:m]
		k = k[m+len(sep):]
	default:
		return iik, errors.New(
			"Unrecognised type tag for inverted index key")
	}

	// docID
	iik.DocID = k[:]

	return iik, nil
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
