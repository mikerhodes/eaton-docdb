package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/pebble"
)

type queryComparison struct {
	key   []string
	value interface{}
	op    string
}

type query struct {
	ands []queryComparison
}

// searchIndex returns IDs matching q.
func searchIndex(indexDb *pebble.DB, q *query) ([]string, error) {
	idsArgumentCount := map[string]int{}
	indexLookupCount := 0

	for _, argument := range q.ands {
		dottedPath := strings.Join(argument.key, ".")

		var ids []string
		var err error
		if argument.op == "=" {
			ids, err = lookupEq(indexDb, dottedPath, argument.value)
		} else if argument.op == ">" {
			ids, err = lookupGT(indexDb, dottedPath, argument.value)
		} else if argument.op == "<" {
			ids, err = lookupLT(indexDb, dottedPath, argument.value)
		} else {
			return nil, errors.New(
				fmt.Sprintf("Unrecognised op %s in query %v", argument.op, q),
			)
		}
		if err != nil {
			return nil, err
		}
		fmt.Printf("op %s ids: %v", argument.op, ids)

		indexLookupCount += 1

		for _, id := range ids {
			_, ok := idsArgumentCount[id]
			if !ok {
				idsArgumentCount[id] = 0
			}

			idsArgumentCount[id]++
		}
	}

	var idsInAll []string
	for id, count := range idsArgumentCount {
		if count == indexLookupCount {
			idsInAll = append(idsInAll, id)
		}
	}

	return idsInAll, nil
}

func lookupEq(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := encodeInvIdxKey(
		[]byte(path), encodeTaggedValue(value), nil)
	endKey := pathValueEndKey(path, value)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		// fmt.Printf("unpacked: %v\n", unpackTuple(iter.Key()))
		id, err := decodeInvIndexKey(iter.Key())
		if err != nil {
			log.Printf("Bad inverted index key found %v: %v", iter.Key(), err)
			continue
		}
		ids = append(ids, string(id.DocID))
	}
	return ids, iter.Close()
}

func lookupGTE(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := encodeInvIdxKey(
		[]byte(path), encodeTaggedValue(value), nil)
	endKey := pathEndKey(path)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		id, err := decodeInvIndexKey(iter.Key())
		if err != nil {
			log.Printf("Bad inverted index key found %v: %v", iter.Key(), err)
			continue
		}
		ids = append(ids, string(id.DocID))
	}
	return ids, iter.Close()
}

func lookupGT(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := encodeInvIdxKey(
		[]byte(path), encodeTaggedValue(value), nil)
	endKey := pathEndKey(path)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// As LowerBound is inclusive, we need to skip over
		// entries at startKey to get greater than semantics.
		if bytes.HasPrefix(iter.Key(), startKey) {
			continue
		}
		// fmt.Printf("key=%+v value=%+v\n", iter.Key(), iter.Value())
		id, err := decodeInvIndexKey(iter.Key())
		if err != nil {
			log.Printf("Bad inverted index key found %v: %v", iter.Key(), err)
			continue
		}
		ids = append(ids, string(id.DocID))
	}
	return ids, iter.Close()
}

func lookupLT(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	// We could use iter.Prev() to get the descending ordering
	ids := []string{}
	startKey := pathStartKey(path)
	endKey := encodeInvIdxKey(
		[]byte(path), encodeTaggedValue(value), nil)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("lessThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		id, err := decodeInvIndexKey(iter.Key())
		if err != nil {
			log.Printf("Bad inverted index key found %v: %v", iter.Key(), err)
			continue
		}
		ids = append(ids, string(id.DocID))
	}
	return ids, iter.Close()
}

func lookupLTE(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	// We could use iter.Prev() to get the descending ordering
	ids := []string{}
	startKey := pathStartKey(path)
	endKey := encodeInvIdxKey(
		[]byte(path), encodeTaggedValue(value), nil)

	// For less than or equal to, we have to explicitly do the
	// equal to search, as UpperBound is exclusive so doesn't
	// include equalTo.
	eqs, err := lookupEq(indexDb, path, value)
	if err != nil {
		return nil, err
	}
	ids = append(ids, eqs...)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("lessThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		id, err := decodeInvIndexKey(iter.Key())
		if err != nil {
			log.Printf("Bad inverted index key found %v: %v", iter.Key(), err)
			continue
		}
		ids = append(ids, string(id.DocID))
	}
	return ids, iter.Close()
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
	buf = append(buf, []byte{invIdxNamespace[0], sep}...)
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
	if k[0] != invIdxNamespace[0] || k[1] != 0 {
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

// pathEndKey returns a key just beyond the end of the range
// of keys for a path
func pathEndKey(path string) []byte {
	// A path's key looks like, for "foo":
	// []byte{102, 111, 111, 0, ... value bytes}
	// to make a key that falls "after" this path foo, place
	// a 1 where the separator would be (1 is always above 0):
	// []byte{102, 111, 111, 1}
	// This relies on the fact that no-one will likely use
	// value 1 in a field name, "Start of Header" character.
	buf := encodeInvIdxKey([]byte(path), nil, nil)
	return append(buf, 1)
}

// pathValueEndKey returns a key just beyond the end of the path-value
// range.
func pathValueEndKey(path string, value interface{}) []byte {
	// Similar to pathEndKey, we use the fact that there's
	// a zero separator between the path value key and the
	// doc ID to generate an upper bound.
	k := encodeInvIdxKey([]byte(path), encodeTaggedValue(value), nil)
	return append(k, 1)
}

// pathStartKey returns a key that is the lowest that
// path could have.
func pathStartKey(path string) []byte {
	buf := encodeInvIdxKey([]byte(path), nil, nil)
	return append(buf, 0)
}
