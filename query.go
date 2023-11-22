package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
)

// searchIndex returns IDs matching q.
func searchIndex(indexDb *pebble.DB, q *query) ([]string, error) {
	idsArgumentCount := map[string]int{}
	nonRangeArguments := 0
	for _, argument := range q.ands {
		var ids []string
		if argument.op == "=" {
			pvk := pathValueAsKey(
				strings.Join(argument.key, "."),
				argument.value,
			)
			// fmt.Printf("Lookup val: %v\n", pvk)

			ids, err := lookupEq(indexDb, pvk)
			if err != nil {
				return nil, err
			}

			fmt.Printf("equalTo ids: %v\n", ids)
		} else if argument.op == ">" {
			ids, err := lookupGE(indexDb, strings.Join(argument.key, "."), argument.value)
			if err != nil {
				return nil, err
			}

			fmt.Printf("greaterThan ids: %v\n", ids)
		} else if argument.op == "<" {
			ids, err := lookupLT(indexDb, strings.Join(argument.key, "."), argument.value)
			if err != nil {
				return nil, err
			}

			fmt.Printf("lessThan ids: %v\n", ids)
		}

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
		if count == nonRangeArguments {
			idsInAll = append(idsInAll, id)
		}
	}

	return idsInAll, nil
}

func lookupEq(indexDb *pebble.DB, pathValue []byte) ([]string, error) {
	idsString, closer, err := indexDb.Get(pathValue)
	if err != nil && err != pebble.ErrNotFound {
		return nil, fmt.Errorf("Could not look up pathvalue [%#v]: %s", pathValue, err)
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
	startKey := pathValueAsKey(path, value)
	endKey := pathEndKey(path)

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
	startKey := pathValueAsKey(path, value)
	endKey := pathEndKey(path)

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
	startKey := pathStartKey(path)
	endKey := pathValueAsKey(path, value)

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
	startKey := pathStartKey(path)
	endKey := pathValueAsKey(path, value)

	// For less than or equal to, we have to explicitly do the
	// equal to search, as UpperBound is exclusive so doesn't
	// include equalTo.
	eqs, err := lookupEq(indexDb, startKey)
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
// This relies on the fact that no-one will likely use
// value 1 in a field name, "Start of Header" character.
func pathEndKey(path string) []byte {
	k := []byte(path)
	pathValue := make([]byte, len(k)+1)
	copy(pathValue[0:], k[0:])
	pathValue[len(k)] = 1 // ie, above the \0 separator
	return pathValue
}

// pathStartKey returns a key that is the lowest that
// path could have.
func pathStartKey(path string) []byte {
	k := []byte(path)
	pathValue := make([]byte, len(k)+1)
	copy(pathValue[0:], k[0:])
	pathValue[len(k)] = 0
	return pathValue

}
