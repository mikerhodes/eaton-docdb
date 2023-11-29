package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getValueAtPath(t *testing.T) {
	tests := []struct {
		object        map[string]any
		path          []string
		expectedValue any
		expectedOk    bool
	}{
		{
			map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			[]string{"a", "b"},
			1,
			true,
		},
		{
			map[string]any{
				"a": map[string]any{
					"b": 1,
				},
			},
			[]string{"a", "c"},
			nil,
			false,
		},
	}

	for _, test := range tests {
		v, ok := getValueAtPath(test.object, test.path)
		assert.Equal(t, test.expectedValue, v)
		assert.Equal(t, test.expectedOk, ok)
	}
}
func Test_simpleSearch(t *testing.T) {
	d := t.TempDir()
	s, err := newServer(d)
	if err != nil {
		assert.FailNow(t, "Could not create s")
	}
	s.addDocument("mike",
		map[string]any{
			"name": "mike",
			"age":  40,
			"pet":  "cat",
		},
	)
	s.addDocument("phil",
		map[string]any{
			"name": "phil",
			"age":  30,
			"pet":  "cat",
		},
	)

	ids, _ := lookupEq(s.indexDb, "name", "mike")
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = lookupEq(s.indexDb, "name", "fred")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupEq(s.indexDb, "age", 40)
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = lookupEq(s.indexDb, "age", "mike")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupEq(s.indexDb, "age", "40")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupEq(s.indexDb, "pet", "cat")
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
}

func Test_lookupGE(t *testing.T) {
	d := t.TempDir()
	s, err := newServer(d)
	if err != nil {
		assert.FailNow(t, "Could not create s")
	}
	s.addDocument("mike",
		map[string]any{
			"name": "mike",
			"age":  40,
			"pet":  "cat",
		},
	)
	s.addDocument("phil",
		map[string]any{
			"name": "phil",
			"age":  30,
			"pet":  "cat",
		},
	)
	s.addDocument("funny",
		map[string]any{
			"name": 1,
			"age":  nil,
			"pet":  false,
		},
	)

	var ids []string

	ids, _ = lookupGE(s.indexDb, "name", "mike")
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = lookupGE(s.indexDb, "name", "ned")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = lookupGE(s.indexDb, "name", "tom")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGE(s.indexDb, "name", 1234)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = lookupGE(s.indexDb, "name", true)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)

	ids, _ = lookupGE(s.indexDb, "age", 20)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = lookupGE(s.indexDb, "age", 40)
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = lookupGE(s.indexDb, "age", 400)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGE(s.indexDb, "age", "mike")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGE(s.indexDb, "age", "40")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupGE(s.indexDb, "pet", "cat")
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)

	// Check we don't bleed into other fields greater than this one
	// Ie, age < name in the byte array prefixes
	ids, _ = lookupGE(s.indexDb, "age", 400000)
	assert.ElementsMatch(t, []string{}, ids)
}

func Test_lookupGT(t *testing.T) {
	d := t.TempDir()
	s, err := newServer(d)
	if err != nil {
		assert.FailNow(t, "Could not create s")
	}
	s.addDocument("mike",
		map[string]any{
			"name": "mike",
			"age":  40,
			"pet":  "cat",
		},
	)
	s.addDocument("phil",
		map[string]any{
			"name": "phil",
			"age":  30,
			"pet":  "cat",
		},
	)
	s.addDocument("funny",
		map[string]any{
			"name": 1,
			"age":  nil,
			"pet":  false,
		},
	)

	var ids []string

	ids, _ = lookupGT(s.indexDb, "name", "mike")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = lookupGT(s.indexDb, "name", "ned")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = lookupGT(s.indexDb, "name", "tom")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGT(s.indexDb, "name", 1234)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = lookupGT(s.indexDb, "name", true)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)

	ids, _ = lookupGT(s.indexDb, "age", 20)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = lookupGT(s.indexDb, "age", 40)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGT(s.indexDb, "age", 400)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGT(s.indexDb, "age", "mike")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = lookupGT(s.indexDb, "age", "40")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupGT(s.indexDb, "pet", "cat")
	assert.ElementsMatch(t, []string{}, ids)

	// Check we don't bleed into other fields greater than this one
	// Ie, age < name in the byte array prefixes
	ids, _ = lookupGT(s.indexDb, "age", 400000)
	assert.ElementsMatch(t, []string{}, ids)
}

func Test_lookupLT(t *testing.T) {
	d := t.TempDir()
	s, err := newServer(d)
	if err != nil {
		assert.FailNow(t, "Could not create s")
	}
	s.addDocument("mike",
		map[string]any{
			"name": "mike",
			"age":  40,
			"pet":  "cat",
		},
	)
	s.addDocument("phil",
		map[string]any{
			"name": "phil",
			"age":  30,
			"pet":  "cat",
		},
	)
	s.addDocument("funny",
		map[string]any{
			"name": 12,
			"age":  nil,
			"pet":  false,
		},
	)

	var ids []string

	ids, _ = lookupLT(s.indexDb, "name", "mike")
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "name", "ned")
	assert.ElementsMatch(t, []string{"mike", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "name", "tom")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "name", 1234)
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "name", true)
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupLT(s.indexDb, "age", 20)
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "age", 40)
	assert.ElementsMatch(t, []string{"phil", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "age", 400)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "age", "mike")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "age", "10")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "age", nil)
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = lookupLT(s.indexDb, "pet", "cat")
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = lookupLT(s.indexDb, "pet", nil)
	assert.ElementsMatch(t, []string{}, ids)

	// Check we don't bleed into other fields lower than this one
	// Ie, name > age in the byte array prefixes
	ids, _ = lookupLT(s.indexDb, "name", 11) // funny is 12
	assert.ElementsMatch(t, []string{}, ids)
}
