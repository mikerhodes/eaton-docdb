package main

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func Test_searchIndex(t *testing.T) {
	d := t.TempDir()
	db, _ := pebble.Open(d, &pebble.Options{})
	index(db, "doc1", map[string]any{
		"a": map[string]any{
			"b": 1,
		},
		"name": "mike",
		"age":  40,
	})
	index(db, "doc2", map[string]any{
		"a": map[string]any{
			"c": 2,
		},
		"name": "john",
		"age":  24,
	})
	index(db, "doc3", map[string]any{
		"a": map[string]any{
			"c": 2,
		},
		"name": "john",
		"age":  110,
	})

	q := &query{
		ands: []queryComparison{
			{[]string{"name"}, "john", "="},
		},
	}
	ids, err := searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatch(t, []string{"doc2", "doc3"}, ids)

	q = &query{
		ands: []queryComparison{
			{[]string{"age"}, 20, ">"},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{"doc1", "doc2", "doc3"}, ids, "%+v", q)

	q = &query{
		ands: []queryComparison{
			{[]string{"age"}, 24, ">"},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{"doc1", "doc3"}, ids, "%+v", q)

	q = &query{
		ands: []queryComparison{
			{[]string{"age"}, 25, ">"},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{"doc1", "doc3"}, ids, "%+v", q)

	q = &query{
		ands: []queryComparison{
			{[]string{"age"}, 120, ">"},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{}, ids, "%+v", q)

	q = &query{
		ands: []queryComparison{
			{[]string{"age"}, 25, ">"},
			{[]string{"name"}, "john", "="},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{"doc3"}, ids, "%+v", q)

	q = &query{
		ands: []queryComparison{
			{[]string{"nonexistentfield"}, 0, ">"},
		},
	}
	ids, err = searchIndex(db, q)
	if err != nil {
		t.Fatalf("Failed due to error: %v", err)
	}
	assert.ElementsMatchf(t, []string{}, ids, "%+v", q)
}

func Test_searchIndexErrors(t *testing.T) {
	d := t.TempDir()
	db, _ := pebble.Open(d, &pebble.Options{})
	index(db, "doc1", map[string]any{
		"a": map[string]any{
			"b": 1,
		},
		"name": "mike",
		"age":  40,
	})
	index(db, "doc2", map[string]any{
		"a": map[string]any{
			"c": 2,
		},
		"name": "john",
		"age":  24,
	})
	index(db, "doc3", map[string]any{
		"a": map[string]any{
			"c": 2,
		},
		"name": "john",
		"age":  110,
	})

	q := &query{
		ands: []queryComparison{
			{[]string{"name"}, "john", "blah="},
		},
	}
	_, err := searchIndex(db, q)
	if err == nil {
		t.Fatalf("Expected error but didn't get one: %v", q)
	}
}
