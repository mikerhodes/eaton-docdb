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
	expected = append(expected, pathValueAsKey("active", false)...)
	k := fwdIdxKey{
		id:           "foo",
		pathValueKey: pathValueAsKey("active", false),
	}
	assert.Equal(t, expected, k.bytes())

	assert.Equal(t, k, decodeFwdIdxKey(expected))
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

	ids, _ := lookupEq(db, "a.b", 1)
	assert.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, ids)

	unindex(db, "doc1")

	ids, _ = lookupEq(db, "a.b", 1)
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

	ids, _ := lookupEq(db, "a.b", 1)
	assert.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, ids)

	doc2 := map[string]any{
		"a": map[string]any{
			"c": 2,
		},
	}
	index(db, "doc2", doc2)

	ids, _ = lookupEq(db, "a.b", 1)
	assert.ElementsMatch(t, []string{"doc1", "doc3"}, ids)
	ids, _ = lookupEq(db, "a.c", 2)
	assert.ElementsMatch(t, []string{"doc2"}, ids)
}
