package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getPath(t *testing.T) {
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
		v, ok := getPath(test.object, test.path)
		assert.Equal(t, test.expectedValue, v)
		assert.Equal(t, test.expectedOk, ok)
	}
}

func Test_makePVI(t *testing.T) {
	tests := []struct {
		path     string
		value    float64
		expected []byte
	}{
		{"a", 12, []byte{
			0x61,                                     // a
			0x0,                                      // null separator
			0x2b,                                     // JSONTagNumber
			0xc0, 0x28, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // float64 12
		}},
		{"a", 13, []byte{
			0x61,                                     // a
			0x0,                                      // null separator
			0x2b,                                     // JSONTagNumber
			0xc0, 0x2a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // float64 13
		}},
		{"a.b.c", 1234567890, []byte{
			0x61, 0x2e, 0x62, 0x2e, 0x63, // a.b.c
			0x0,                                          // null separator
			0x2b,                                         // JSONTagNumber
			0xc1, 0xd2, 0x65, 0x80, 0xb4, 0x80, 0x0, 0x0, // float 1234567890
		}},
	}

	for _, test := range tests {
		got := makePVI(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makePVS(t *testing.T) {
	tests := []struct {
		path     string
		value    string
		expected []byte
	}{
		{"a", "foo", []byte{
			0x61,             // a
			0x0,              // null separator
			0x2c,             // JSONTagString
			0x66, 0x6f, 0x6f, // foo
		}},
		{"b", "fop", []byte{
			0x62,             // a
			0x0,              // null separator
			0x2c,             // JSONTagString
			0x66, 0x6f, 0x70, // fop
		}},
		{"a.b.c", "hello world Im here", []byte{
			0x61, 0x2e, 0x62, 0x2e, 0x63, // a.b.c
			0x0,                                      // null separator
			0x2c,                                     // JSONTagString
			0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, // hello world Im here
			0x6f, 0x72, 0x6c, 0x64, 0x20, 0x49, 0x6d,
			0x20, 0x68, 0x65, 0x72, 0x65,
		}},
	}

	for _, test := range tests {
		got := makePVS(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makePVB(t *testing.T) {
	tests := []struct {
		path     string
		value    bool
		expected []byte
	}{
		{"a", true, []byte{
			0x61, // a
			0x0,  // null separator
			0x2a, // JSONTagTrue
		}},
		{"b", false, []byte{
			0x62, // a
			0x0,  // null separator
			0x29, // JSONTagFalse
		}},
		{"a.b.c", false, []byte{
			0x61, 0x2e, 0x62, 0x2e, 0x63, // a.b.c
			0x0,  // null separator
			0x29, // JSONTagFalse
		}},
	}

	for _, test := range tests {
		got := makePVB(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makePVN(t *testing.T) {
	tests := []struct {
		path     string
		expected []byte
	}{
		{"a", []byte{
			0x61, // a
			0x0,  // null separator
			0x28, // JSONTagNull
		}},
		{"b", []byte{
			0x62, // a
			0x0,  // null separator
			0x28, // JSONTagNull
		}},
		{"a.b.c", []byte{
			0x61, 0x2e, 0x62, 0x2e, 0x63, // a.b.c
			0x0,  // null separator
			0x28, // JSONTagNull
		}},
	}

	for _, test := range tests {
		got := makePVN(test.path)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, nil)
	}
}

func Test_getPathValues(t *testing.T) {
	tests := []struct {
		obj         map[string]any
		prefix      string
		expectedPvs [][]byte
	}{
		{
			map[string]any{"a": 2, "b": 4, "c": "hey world"},
			"",
			[][]byte{makePVI("a", 2), makePVI("b", 4), makePVS("c", "hey world")},
		},
		{
			map[string]any{"a": map[string]any{"12": "foo"}},
			"",
			[][]byte{makePVS("a.12", "foo")},
		},
	}

	for _, test := range tests {
		pvs := getPathValues(test.obj, test.prefix)
		assert.Equal(t, len(test.expectedPvs), len(pvs))
		assert.ElementsMatch(t, test.expectedPvs, pvs)
	}
}

// TODO tests for PV sort ordering
func Test_makePVISort(t *testing.T) {
	tests := []struct {
		l float64
		h float64
	}{
		{11, 12},
		{1, 100},
		{-1, 100},
		{45, 4500000},
		{-4500000, 4500000},
	}
	for _, test := range tests {
		h := makePVI("a", test.h)
		l := makePVI("a", test.l)
		assert.True(t, slices.Compare(h, l) > 0,
			"%v %v !> %v %v", h, test.h, test.l, l)
	}
}

func Test_makePVSSort(t *testing.T) {
	tests := []struct {
		l string
		h string
	}{
		{"1", "5"},
		{"a", "b"},
		{"foooo", "fooop"},
		{"foooooooooooooo", "fooop"},
		{"a whole lot of string", "a whole lot of text"},
	}
	for _, test := range tests {
		h := makePVS("a", test.h)
		l := makePVS("a", test.l)
		assert.True(t, slices.Compare(h, l) > 0,
			"%v %s !> %s %v", h, test.h, test.l, l)
	}
}

// Check against the CouchDB collation order.
func Test_PVSortTypes(t *testing.T) {
	p := "a.b.c"
	assert.True(t, slices.Compare(makePVN(p), makePVB(p, false)) < 0,
		"null should be less than false")
	assert.True(t, slices.Compare(makePVB(p, false), makePVB(p, true)) < 0,
		"false should be less than true")
	assert.True(t, slices.Compare(makePVB(p, true), makePVI(p, 1234)) < 0,
		"true should be less than number")
	assert.True(t, slices.Compare(makePVI(p, 1234), makePVS(p, "1234")) < 0,
		"number should be less than string")
}

func Test_pathValueAsKey(t *testing.T) {
	// pathvalueaskey is a shorthand to get the right makePV*, so we
	// can compare against the known-good results for those lower level
	// methods.
	assert.True(t,
		slices.Compare(makePVN("a"), pathValueAsKey("a", nil)) == 0,
		"Failed for null",
	)
	assert.True(t,
		slices.Compare(makePVB("a", false), pathValueAsKey("a", false)) == 0,
		"Failed for false",
	)
	assert.True(t,
		slices.Compare(makePVB("a", true), pathValueAsKey("a", true)) == 0,
		"Failed for true",
	)
	assert.True(t,
		slices.Compare(makePVI("a", 1234), pathValueAsKey("a", 1234)) == 0,
		"Failed for number",
	)
	assert.True(t,
		slices.Compare(makePVS("a", "b"), pathValueAsKey("a", "b")) == 0,
		"Failed for string",
	)

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

	ids, _ := s.lookup(makePVS("name", "mike"))
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = s.lookup(makePVS("name", "fred"))
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookup(makePVI("age", 40))
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = s.lookup(makePVS("age", "mike"))
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookup(makePVS("age", "40"))
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookup(makePVS("pet", "cat"))
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

	ids, _ = s.lookupGE("name", "mike")
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = s.lookupGE("name", "ned")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = s.lookupGE("name", "tom")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGE("name", 1234)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = s.lookupGE("name", true)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)

	ids, _ = s.lookupGE("age", 20)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = s.lookupGE("age", 40)
	assert.ElementsMatch(t, []string{"mike"}, ids)
	ids, _ = s.lookupGE("age", 400)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGE("age", "mike")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGE("age", "40")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookupGE("pet", "cat")
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)

	// Check we don't bleed into other fields greater than this one
	// Ie, age < name in the byte array prefixes
	ids, _ = s.lookupGE("age", 400000)
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

	ids, _ = s.lookupGT("name", "mike")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = s.lookupGT("name", "ned")
	assert.ElementsMatch(t, []string{"phil"}, ids)
	ids, _ = s.lookupGT("name", "tom")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGT("name", 1234)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = s.lookupGT("name", true)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)

	ids, _ = s.lookupGT("age", 20)
	assert.ElementsMatch(t, []string{"mike", "phil"}, ids)
	ids, _ = s.lookupGT("age", 40)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGT("age", 400)
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGT("age", "mike")
	assert.ElementsMatch(t, []string{}, ids)
	ids, _ = s.lookupGT("age", "40")
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookupGT("pet", "cat")
	assert.ElementsMatch(t, []string{}, ids)

	// Check we don't bleed into other fields greater than this one
	// Ie, age < name in the byte array prefixes
	ids, _ = s.lookupGT("age", 400000)
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

	ids, _ = s.lookupLT("name", "mike")
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = s.lookupLT("name", "ned")
	assert.ElementsMatch(t, []string{"mike", "funny"}, ids)
	ids, _ = s.lookupLT("name", "tom")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = s.lookupLT("name", 1234)
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = s.lookupLT("name", true)
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookupLT("age", 20)
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = s.lookupLT("age", 40)
	assert.ElementsMatch(t, []string{"phil", "funny"}, ids)
	ids, _ = s.lookupLT("age", 400)
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = s.lookupLT("age", "mike")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = s.lookupLT("age", "10")
	assert.ElementsMatch(t, []string{"mike", "phil", "funny"}, ids)
	ids, _ = s.lookupLT("age", nil)
	assert.ElementsMatch(t, []string{}, ids)

	ids, _ = s.lookupLT("pet", "cat")
	assert.ElementsMatch(t, []string{"funny"}, ids)
	ids, _ = s.lookupLT("pet", nil)
	assert.ElementsMatch(t, []string{}, ids)

	// Check we don't bleed into other fields lower than this one
	// Ie, name > age in the byte array prefixes
	ids, _ = s.lookupLT("name", 11) // funny is 12
	assert.ElementsMatch(t, []string{}, ids)
}
