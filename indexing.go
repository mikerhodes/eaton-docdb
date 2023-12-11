package main

import (
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
)

var invIdxNamespace []byte = []byte{'i'}
var fwdIdxNamespace []byte = []byte{'f'}

func invIdxKey(pathValueKey []byte, id *string) []byte {
	if id != nil {
		return packTuple(invIdxNamespace, pathValueKey, []byte(*id))
	} else {
		return packTuple(invIdxNamespace, pathValueKey)
	}
}

// index adds document to the index, associated with id.
func index(indexDB *pebble.DB, id string, document map[string]any) {
	// First, unindex the document
	err := unindex(indexDB, id)
	if err != nil {
		log.Printf("Could not unindex %q: %v", id, err)
		return
	}

	// Now, index the new values for the document
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		invIdxKey := invIdxKey(pathValue, &id)

		err = indexDB.Set(invIdxKey, nil, pebble.Sync)
		if err != nil {
			log.Printf("Could not update inverted index: %s", err)
		}

		// Create the fwd index entries for this field of the document
		fwdIdxKey := fwdIdxKey{
			id:           id,
			pathValueKey: pathValue,
		}
		err = indexDB.Set(fwdIdxKey.bytes(), []byte{}, pebble.Sync)
		log.Printf("fwd key bytes: %v", fwdIdxKey.bytes())
		if err != nil {
			log.Printf("Could not update forward index: %s", err)
		}

	}
}

// unindex removes index entries for id from indexDb
func unindex(indexDb *pebble.DB, id string) error {
	// To unindex, we use the forward index (id -> pathValueKeys) to
	// find all the keys in the inverted index to remove. After removing
	// those, we clean up the forward index.

	b := indexDb.NewIndexedBatch()

	// 1. Get the range for id from the forward index. Everything
	//    is encoded into the keys.
	startKey := packTuple(fwdIdxNamespace, []byte(id))
	endKey := packTuple(fwdIdxNamespace, []byte(id))
	endKey = append(endKey, 1) // 1 > 0-separator

	// 2. Read all the keys. Deserialise each key to find the
	//    pathValueKey that is in the inverted index, and delete
	//    that from the inverted index.
	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	iter := b.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fik := NewFwdIdxKey(iter.Key())
		log.Printf("unindex fwd key bytes: %v", fik.bytes())
		log.Printf("fik: %+v", fik)
		invIdxKey := invIdxKey(fik.pathValueKey, &fik.id)
		err := b.Delete(invIdxKey, pebble.Sync)
		if err != nil {
			log.Printf(
				"Couldn't delete/set invIdxKey %v in index: %v",
				invIdxKey, err)
		}
	}
	iter.Close()

	// 3. Remove all the entries for id in the forward index.
	b.DeleteRange(startKey, endKey, pebble.Sync)

	return indexDb.Apply(b, pebble.Sync)
}

type fwdIdxKey struct {
	id           string
	pathValueKey []byte
}

// NewFwdIdxKey deserialises a fwdIndexKey from b
func NewFwdIdxKey(b []byte) fwdIdxKey {
	// We need to use unpackTupleN because pathValueKey is
	// itself a tuple with multiple components, and we don't
	// want to split that.
	parts := unpackTupleN(b, 3)
	return fwdIdxKey{
		id:           string(parts[1]),
		pathValueKey: parts[2],
	}
}

// bytes serialises a fwdIndexKey to bytes
func (k fwdIdxKey) bytes() []byte {
	return packTuple(fwdIdxNamespace, []byte(k.id), k.pathValueKey)
}

// ensureIdInValue ensures that id is in idsString, which is the
// value we store in the index, a comma-separated list of doc IDs.
func ensureIdInValue(idsString []byte, id string) []byte {
	if len(idsString) == 0 {
		idsString = []byte(id)
	} else {
		ids := strings.Split(string(idsString), ",")

		found := false
		for _, existingId := range ids {
			if id == existingId {
				found = true
				break
			}
		}

		if !found {
			idsString = append(idsString, []byte(","+id)...)
		}
	}
	return idsString
}

func deleteIdFromValue(idsString []byte, id string) []byte {
	if len(idsString) == 0 {
		return idsString
	} else {
		ids := strings.Split(string(idsString), ",")
		ids = slices.DeleteFunc(ids, func(E string) bool {
			return E == id
		})
		return []byte(strings.Join(ids, ","))
	}
}

// getPathValues returns all path value keys for obj, using prefix as
// key prefix for the path part of the key.
// Ignores arrays
func getPathValues(obj map[string]any, prefix string) [][]byte {
	var pvs [][]byte
	for key, val := range obj {
		switch t := val.(type) {
		case map[string]any:
			pvs = append(pvs, getPathValues(t, key)...)
			continue
		case []interface{}:
			// Can't handle arrays
			continue
		}

		if prefix != "" {
			key = prefix + "." + key
		}

		pvk := pathValueAsKey(key, val)

		fmt.Printf("Added index val: %v\n", pvk)

		pvs = append(pvs, pvk)
	}

	return pvs
}
