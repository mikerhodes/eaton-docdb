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
