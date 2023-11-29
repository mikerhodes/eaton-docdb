package main

import (
	"encoding/json"
	"log"

	"github.com/cockroachdb/pebble"
	// "github.com/google/uuid"
)

// Tags for the values of JSON
// primitives. Objects and arrays
// are encoded into the path.
// Tags are inserted prior to the
// values in pathValues for keys to
// ensure sort ordering.
const (
	// printable character makes easier debugging
	JSONTagNull   = iota + 40 // 0x28 (
	JSONTagFalse              // 0x29 )
	JSONTagTrue               // 0x2a *
	JSONTagNumber             // 0x2b +
	JSONTagString             // 0x2c ,
)

type server struct {
	db      *pebble.DB // Primary data
	indexDb *pebble.DB // Index data
}

// newServer returns a new database server with data on disk
// at database.
func newServer(database string) (*server, error) {
	s := server{db: nil}
	var err error
	s.db, err = pebble.Open(database, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	s.indexDb, err = pebble.Open(database+".index", &pebble.Options{})
	return &s, err
}

// addDocument adds and indexes document with id.
func (s server) addDocument(id string, document map[string]any) error {
	// New unique id for the document
	// id := uuid.New().String()

	index(s.indexDb, id, document)

	bs, err := json.Marshal(document)
	if err != nil {
		return err
	}
	err = s.db.Set([]byte(id), bs, pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

// getDocumentById returns a document for id, if it exists in the
// database.
func (s server) getDocumentById(id []byte) (map[string]any, error) {
	valBytes, closer, err := s.db.Get(id)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var document map[string]any
	err = json.Unmarshal(valBytes, &document)
	return document, err
}

// reindex adds all documents in primary data to the index
func (s server) reindex() {
	iter := s.db.NewIter(nil)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var document map[string]any
		err := json.Unmarshal(iter.Value(), &document)
		if err != nil {
			log.Printf("Unable to parse bad document, %s: %s", string(iter.Key()), err)
		}
		index(s.indexDb, string(iter.Key()), document)
	}
}

// func (s server) searchDocuments(q *query, skipIndex bool) (map[string]interface{}, error) {

// 	var idsInAll []string = nil
// 	isRange := false

// 	if !skipIndex {
// 		idsInAll, _ = searchIndex(s.indexDb, q)
// 	}

// 	var documents []any

// 	if len(idsInAll) > 0 {
// 		for _, id := range idsInAll {
// 			document, err := s.getDocumentById([]byte(id))
// 			if err != nil {
// 				return nil, err
// 			}

// 			if !isRange || q.match(document) {
// 				documents = append(documents, map[string]any{
// 					"id":   id,
// 					"body": document,
// 				})
// 			}
// 		}
// 	} else {
// 		iter := s.db.NewIter(nil)
// 		defer iter.Close()
// 		for iter.First(); iter.Valid(); iter.Next() {
// 			var document map[string]any
// 			err := json.Unmarshal(iter.Value(), &document)
// 			if err != nil {
// 				return nil, err
// 			}

// 			if q.match(document) {
// 				documents = append(documents, map[string]any{
// 					"id":   string(iter.Key()),
// 					"body": document,
// 				})
// 			}
// 		}
// 	}

// 	return map[string]any{"documents": documents, "count": len(documents)}, nil
// }

// getValueAtPath returns the value at path parts for doc. If not found,
// returns nil, false.
func getValueAtPath(doc map[string]any, parts []string) (any, bool) {
	var docSegment any = doc
	for _, part := range parts {
		m, ok := docSegment.(map[string]any)
		if !ok {
			return nil, false
		}

		if docSegment, ok = m[part]; !ok {
			return nil, false
		}
	}

	return docSegment, true
}

// match returns true if this query q matches doc
// func (q query) match(doc map[string]any) bool {
// 	for _, argument := range q.ands {
// 		value, ok := getValueAtPath(doc, argument.key)
// 		if !ok {
// 			return false
// 		}

// 		// Handle equality
// 		if argument.op == "=" {
// 			match := fmt.Sprintf("%v", value) == argument.value
// 			if !match {
// 				return false
// 			}

// 			continue
// 		}

// 		// Handle <, >
// 		right, err := strconv.ParseFloat(argument.value, 64)
// 		if err != nil {
// 			return false
// 		}

// 		var left float64
// 		switch t := value.(type) {
// 		case float64:
// 			left = t
// 		case float32:
// 			left = float64(t)
// 		case uint:
// 			left = float64(t)
// 		case uint8:
// 			left = float64(t)
// 		case uint16:
// 			left = float64(t)
// 		case uint32:
// 			left = float64(t)
// 		case uint64:
// 			left = float64(t)
// 		case int:
// 			left = float64(t)
// 		case int8:
// 			left = float64(t)
// 		case int16:
// 			left = float64(t)
// 		case int32:
// 			left = float64(t)
// 		case int64:
// 			left = float64(t)
// 		case string:
// 			left, err = strconv.ParseFloat(t, 64)
// 			if err != nil {
// 				return false
// 			}
// 		default:
// 			return false
// 		}

// 		if argument.op == ">" {
// 			if left <= right {
// 				return false
// 			}

// 			continue
// 		}

// 		if left >= right {
// 			return false
// 		}
// 	}

// 	return true
// }

func main() {
	s, err := newServer("docdb.data")
	if err != nil {
		log.Fatal(err)
	}
	defer s.db.Close()

	s.reindex()
}
