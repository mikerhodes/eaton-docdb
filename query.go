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
