package main

import (
	"log"

	"github.com/cockroachdb/pebble"
)

// This file contains the two key functions for maintaining the index:
// - index
// - unindex
//
// Indexing grabs each of the "path values" for the JSON document,
// a combination of the dotted path of field names and the value
// at the path. The value can be of various types, so is encoded
// with a tag to identify the type (see encodeValue).
// When indexing, we create an inverted index key (invIdxKey variable
// usually). The form of this key can be seen in encodeInvIdxKey.
// It's constructed to make it quick to look up values in particular
// fields and return the IDs that match.
//
// When indexing, we also create a "forward index", where we create
// a second index structure that allows us to look up all entries
// in the inverted index for a given doc ID. This can be seen in
// fwdIdxKey. This is needed so that we can delete and update
// documents, because it allows us to know what keys the document
// was previously indexed under.
//
// When we delete a document, we use the forward index
// to look up all the keys to delete in the inverted index, then
// clean up the forward index.
//
// When updating, we use the forward index to first remove the
// existing forward index keys, then proceed to index the new
// document content from scratch. We could decrease the write
// amplication that this creates by instead calculating a
// delta to apply to the inverted index, but for now we don't.
//
// The inverted and forward indexes are stored in the same
// Pebble database, we use a key prefix to namespace the two
// indexes.

var invIdxNamespace []byte = []byte{'i'}
var fwdIdxNamespace []byte = []byte{'f'}

// index adds document to the index, associated with id.
func index(indexDB *pebble.DB, id string, document map[string]any) {
	docID := []byte(id)
	// First, unindex the document
	err := unindex(indexDB, docID)
	if err != nil {
		log.Printf("Could not unindex %q: %v", id, err)
		return
	}

	b := indexDB.NewBatch()

	// Now, index the new values for the document
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		invIdxKey := encodeInvIdxKey(
			pathValue.path, pathValue.taggedValue, docID)

		err = b.Set(invIdxKey, nil, pebble.Sync)
		if err != nil {
			log.Printf("Could not update inverted index: %s", err)
		}

		// Create the fwd index entries for this field of the document
		fwdIdxKey := fwdIdxKey{
			id:          docID,
			path:        pathValue.path,
			taggedValue: pathValue.taggedValue,
		}
		err = b.Set(encodeFwdIdxKey(fwdIdxKey), []byte{}, pebble.Sync)
		// log.Printf("fwd key bytes: %v", encodeFwdIdxKey(fwdIdxKey))
		if err != nil {
			log.Printf("Could not update forward index: %s", err)
		}
	}

	err = indexDB.Apply(b, pebble.Sync)
	if err != nil {
		log.Printf("Could not index %q: %v", id, err)
		return
	}
}

// unindex removes index entries for id from indexDb
func unindex(indexDb *pebble.DB, docID []byte) error {
	// To unindex, we use the forward index (id -> pathValueKeys) to
	// find all the keys in the inverted index to remove. After removing
	// those, we clean up the forward index.

	b := indexDb.NewIndexedBatch()

	// 1. Get the range for id from the forward index. Everything
	//    is encoded into the keys.
	startKey := packTuple(fwdIdxNamespace, docID)
	endKey := packTuple(fwdIdxNamespace, docID)
	endKey = append(endKey, 1) // 1 > 0-separator

	// 2. Read all the keys. Deserialise each key to find the
	//    pathValueKey that is in the inverted index, and delete
	//    that from the inverted index.
	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	iter := b.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fik := decodeFwdIdxKey(iter.Key())
		// log.Printf("unindex fwd key bytes: %v", encodeFwdIdxKey(fik))
		// log.Printf("fik: %+v", fik)
		invIdxKey := encodeInvIdxKey(fik.path, fik.taggedValue, fik.id)
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

type pathValue struct {
	path, taggedValue []byte
}

// getPathValues returns all path value keys for obj, using prefix as
// key prefix for the path part of the key.
// Ignores arrays
func getPathValues(obj map[string]any, prefix string) []pathValue {
	var pvs []pathValue
	for key, val := range obj {
		if prefix != "" {
			key = prefix + "." + key
		}

		switch t := val.(type) {
		case map[string]any:
			pvs = append(pvs, getPathValues(t, key)...)
			continue
		case []interface{}:
			// Can't handle arrays
			continue
		}

		pvk := pathValue{[]byte(key), encodeTaggedValue(val)}
		// fmt.Printf("Added index val: %v\n", pvk)
		pvs = append(pvs, pvk)
	}

	return pvs
}
