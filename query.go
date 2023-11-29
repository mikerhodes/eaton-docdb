package main

import (
	"errors"
	"fmt"
	"log"
	"slices"
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
	pvk := pathValueAsKey(path, value)
	indexKey := invIdxKey(pvk)
	log.Printf("lookupEq: %v", indexKey)
	idsString, closer, err := indexDb.Get(indexKey)
	if err != nil && err != pebble.ErrNotFound {
		return nil, fmt.Errorf("Could not look up pathvalue [%#v]: %s", pvk, err)
	}
	if closer != nil {
		defer closer.Close()
	}

	if len(idsString) == 0 {
		return nil, nil
	}

	return strings.Split(string(idsString), ","), nil
}

func lookupGE(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := invIdxKey(pathValueAsKey(path, value))
	endKey := invIdxKey(pathEndKey(path))

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

func lookupGT(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := invIdxKey(pathValueAsKey(path, value))
	endKey := invIdxKey(pathEndKey(path))

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		// As LowerBound is inclusive, we need to skip over
		// entries at startKey to get greater than semantics.
		if slices.Compare(startKey, iter.Key()) == 0 {
			continue
		}
		fmt.Printf("key=%+v value=%+v\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

func lookupLT(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	// We could use iter.Prev() to get the descending ordering
	ids := []string{}
	startKey := invIdxKey(pathStartKey(path))
	endKey := invIdxKey(pathValueAsKey(path, value))

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("lessThan: %+v\n", readOptions)

	iter := indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

func lookupLE(indexDb *pebble.DB, path string, value interface{}) ([]string, error) {
	// We could use iter.Prev() to get the descending ordering
	ids := []string{}
	startKey := invIdxKey(pathStartKey(path))
	endKey := invIdxKey(pathValueAsKey(path, value))

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
		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

// pathEndKey returns a key just beyond the end of the path
func pathEndKey(path string) []byte {
	// A path's key looks like, for "foo":
	// []byte{102, 111, 111, 0, ... value bytes}
	// to make a key that falls "after" this path foo, place
	// a 1 where the separator would be (1 is always above 0):
	// []byte{102, 111, 111, 1}
	// This relies on the fact that no-one will likely use
	// value 1 in a field name, "Start of Header" character.
	k := []byte(path)
	return append(k, 1)
}

// pathStartKey returns a key that is the lowest that
// path could have.
func pathStartKey(path string) []byte {
	k := []byte(path)
	return append(k, 0)
}
