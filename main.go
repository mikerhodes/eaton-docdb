package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

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

		pvk := pathValueAsKey(key, fmt.Sprintf("%v", val))

		fmt.Printf("Added index val: %v\n", pvk)

		pvs = append(pvs, pvk)
	}

	return pvs
}

func (s server) index(id string, document map[string]any) {
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		idsString, closer, err := s.indexDb.Get([]byte(pathValue))
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
		err = s.indexDb.Set(pathValue, idsString, pebble.Sync)
		if err != nil {
			log.Printf("Could not update index: %s", err)
		}
	}
}

func (s server) addDocument(id string, document map[string]any) error {

	// New unique id for the document
	// id := uuid.New().String()

	s.index(id, document)

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

type queryComparison struct {
	key   []string
	value string
	op    string
}

type query struct {
	ands []queryComparison
}

func getPath(doc map[string]any, parts []string) (any, bool) {
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

func (q query) match(doc map[string]any) bool {
	for _, argument := range q.ands {
		value, ok := getPath(doc, argument.key)
		if !ok {
			return false
		}

		// Handle equality
		if argument.op == "=" {
			match := fmt.Sprintf("%v", value) == argument.value
			if !match {
				return false
			}

			continue
		}

		// Handle <, >
		right, err := strconv.ParseFloat(argument.value, 64)
		if err != nil {
			return false
		}

		var left float64
		switch t := value.(type) {
		case float64:
			left = t
		case float32:
			left = float64(t)
		case uint:
			left = float64(t)
		case uint8:
			left = float64(t)
		case uint16:
			left = float64(t)
		case uint32:
			left = float64(t)
		case uint64:
			left = float64(t)
		case int:
			left = float64(t)
		case int8:
			left = float64(t)
		case int16:
			left = float64(t)
		case int32:
			left = float64(t)
		case int64:
			left = float64(t)
		case string:
			left, err = strconv.ParseFloat(t, 64)
			if err != nil {
				return false
			}
		default:
			return false
		}

		if argument.op == ">" {
			if left <= right {
				return false
			}

			continue
		}

		if left >= right {
			return false
		}
	}

	return true
}

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

func (s server) lookup(pathValue []byte) ([]string, error) {
	idsString, closer, err := s.indexDb.Get(pathValue)
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

func (s server) greaterThanLookup(path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := pathValueAsKey(path, fmt.Sprintf("%v", value))
	endKey := pathEndKey(path)

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("greaterThan: %+v\n", readOptions)

	iter := s.indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

func (s server) lessThanLookup(path string, value interface{}) ([]string, error) {
	ids := []string{}
	startKey := pathStartKey(path)
	endKey := pathValueAsKey(path, fmt.Sprintf("%v", value))

	readOptions := &pebble.IterOptions{LowerBound: startKey, UpperBound: endKey}
	fmt.Printf("lessThan: %+v\n", readOptions)

	iter := s.indexDb.NewIter(readOptions)
	for iter.SeekGE(startKey); iter.Valid(); iter.Next() {
		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
		ids = append(
			ids,
			strings.Split(string(iter.Value()), ",")...)
	}
	return ids, iter.Close()
}

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
func pathValueAsKey(path string, value string) []byte {
	fmt.Printf("path: %+v, value: %+v\n", path, value)

	// For int values, not floats for now, convert into
	// an int64.
	// TODO right now this will also encode a JSON string
	// of 45 as an int, which we shouldn't do...
	i, err := strconv.Atoi(value)
	if err == nil {
		return makePVI(path, float64(i))
	} else {
		return makePVS(path, value)
	}
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

func (s server) searchIndex(q *query) ([]string, bool, error) {
	idsArgumentCount := map[string]int{}
	nonRangeArguments := 0
	for _, argument := range q.ands {
		var ids []string
		if argument.op == "=" {
			pvk := pathValueAsKey(
				strings.Join(argument.key, "."),
				argument.value,
			)
			fmt.Printf("Lookup val: %v\n", pvk)

			ids, err := s.lookup(pvk)
			if err != nil {
				return nil, false, err
			}

			fmt.Printf("equalTo ids: %v\n", ids)
		} else if argument.op == ">" {
			ids, err := s.greaterThanLookup(strings.Join(argument.key, "."), argument.value)
			if err != nil {
				return nil, false, err
			}

			fmt.Printf("greaterThan ids: %v\n", ids)
		} else if argument.op == "<" {
			ids, err := s.lessThanLookup(strings.Join(argument.key, "."), argument.value)
			if err != nil {
				return nil, false, err
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

	return idsInAll, false, nil
}

func (s server) searchDocuments(q *query, skipIndex bool) (map[string]interface{}, error) {

	idsInAll, isRange, _ := s.searchIndex(q)

	var documents []any
	if skipIndex {
		idsInAll = nil
	}
	if len(idsInAll) > 0 {
		for _, id := range idsInAll {
			document, err := s.getDocumentById([]byte(id))
			if err != nil {
				return nil, err
			}

			if !isRange || q.match(document) {
				documents = append(documents, map[string]any{
					"id":   id,
					"body": document,
				})
			}
		}
	} else {
		iter := s.db.NewIter(nil)
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			var document map[string]any
			err := json.Unmarshal(iter.Value(), &document)
			if err != nil {
				return nil, err
			}

			if q.match(document) {
				documents = append(documents, map[string]any{
					"id":   string(iter.Key()),
					"body": document,
				})
			}
		}
	}

	return map[string]any{"documents": documents, "count": len(documents)}, nil
}

func (s server) reindex() {
	iter := s.db.NewIter(nil)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var document map[string]any
		err := json.Unmarshal(iter.Value(), &document)
		if err != nil {
			log.Printf("Unable to parse bad document, %s: %s", string(iter.Key()), err)
		}
		s.index(string(iter.Key()), document)
	}
}

func main() {
	s, err := newServer("docdb.data")
	if err != nil {
		log.Fatal(err)
	}
	defer s.db.Close()

	s.reindex()
}
