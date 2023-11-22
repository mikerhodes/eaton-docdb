package main

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func Test_fwdIndexKey(t *testing.T) {
	expected := []byte{
		0x66,             // f, fwdIdxNamespace
		0x0,              // separator
		0x66, 0x6f, 0x6f, // foo
		0x0, // separator
	}
	expected = append(expected, makePVB("active", false)...)
	k := fwdIdxKey{
		id:           "foo",
		pathValueKey: makePVB("active", false),
	}
	assert.Equal(t, expected, k.bytes())

	assert.Equal(t, k, NewFwdIdxKey(expected))
}

func Test_unindex_deleteIdFromValue(t *testing.T) {
	assert.Equal(t,
		string([]byte("doc2")),
		string(deleteIdFromValue([]byte("doc2,doc1"), "doc1")),
	)
	assert.Equal(t,
		string([]byte("doc2")),
		string(deleteIdFromValue([]byte("doc1,doc2"), "doc1")),
	)
	assert.Equal(t,
		[]byte("doc1,doc3"),
		deleteIdFromValue([]byte("doc1,doc2,doc2,doc3"), "doc2"),
	)
	assert.Equal(t,
		[]byte("doc2"),
		deleteIdFromValue([]byte("doc1,doc2,doc1"), "doc1"),
	)
	assert.Equal(t,
		[]byte("doc1,doc2,doc4,doc5,doc6"),
		deleteIdFromValue([]byte("doc1,doc2,doc3,doc4,doc5,doc3,doc6"), "doc3"),
	)
}

func Test_unindex(t *testing.T) {
	d := t.TempDir()
	db, _ := pebble.Open(d, &pebble.Options{})
	doc := map[string]any{
		"a": map[string]any{
			"b": 1,
		},
	}
	index(db, "doc1", doc)
	index(db, "doc2", doc)
	index(db, "doc3", doc)

	ids, _ := lookupEq(db, makePVI("a.b", 1))
	assert.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, ids)

	unindex(db, "doc1")

	ids, _ = lookupEq(db, makePVI("a.b", 1))
	assert.ElementsMatch(t, []string{"doc2", "doc3"}, ids)
}

// Tests that if we index the same document, it's values
// are updated in the index (at least from the point of
// view of searches).
func Test_reindex(t *testing.T) {
	d := t.TempDir()
	db, _ := pebble.Open(d, &pebble.Options{})
	doc := map[string]any{
		"a": map[string]any{
			"b": 1,
		},
	}
	index(db, "doc1", doc)
	index(db, "doc2", doc)
	index(db, "doc3", doc)

	ids, _ := lookupEq(db, makePVI("a.b", 1))
	assert.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, ids)

	doc2 := map[string]any{
		"a": map[string]any{
			"c": 2,
		},
	}
	index(db, "doc2", doc2)

	ids, _ = lookupEq(db, makePVI("a.b", 1))
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, ids)
	ids, _ = lookupEq(db, makePVI("a.c", 2))
	assert.ElementsMatch(t, []string{"doc2"}, ids)
}
