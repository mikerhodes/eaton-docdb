package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/pebble"
)

// index adds document to the index, associated with id.
func index(indexDB *pebble.DB, id string, document map[string]any) {
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		idsString, closer, err := indexDB.Get([]byte(pathValue))
		if err != nil && err != pebble.ErrNotFound {
			log.Printf("Could not look up pathvalue [%#v]: %s", document, err)
		}

		if len(idsString) == 0 {
			idsString = []byte(id)
		} else {
			ids := strings.Split(string(idsString), ",")

			found := false
			for _, existingId := range ids {
				if id == existingId {
					found = true
				}
			}

			if !found {
				idsString = append(idsString, []byte(","+id)...)
			}
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				log.Printf("Could not close: %s", err)
			}
		}
		err = indexDB.Set(pathValue, idsString, pebble.Sync)
		if err != nil {
			log.Printf("Could not update index: %s", err)
		}
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
