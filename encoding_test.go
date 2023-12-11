package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_makeFloatKey(t *testing.T) {
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
		{"a.b.c", -1, []byte{
			0x61, 0x2e, 0x62, 0x2e, 0x63, // a.b.c
			0x0,                                            // null separator
			0x2b,                                           // JSONTagNumber
			0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // float 1234567890
		}},
	}

	for _, test := range tests {
		got := pathValueAsKey(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makeStringKey(t *testing.T) {
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
		got := pathValueAsKey(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makeBooleanKey(t *testing.T) {
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
		got := pathValueAsKey(test.path, test.value)
		assert.Equal(t, test.expected, got, "%s=%s", test.path, test.value)
	}
}

func Test_makeNullKey(t *testing.T) {
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
		got := pathValueAsKey(test.path, nil)
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
			[][]byte{pathValueAsKey("a", 2), pathValueAsKey("b", 4), pathValueAsKey("c", "hey world")},
		},
		{
			map[string]any{"a": map[string]any{"12": "foo"}},
			"",
			[][]byte{pathValueAsKey("a.12", "foo")},
		},
		{
			map[string]any{"a": map[string]any{
				"b": map[string]any{
					"c":   map[string]any{"d": "foo"},
					"foo": "bar",
				},
			}},
			"",
			[][]byte{
				pathValueAsKey("a.b.c.d", "foo"),
				pathValueAsKey("a.b.foo", "bar"),
			},
		},
	}

	for _, test := range tests {
		pvs := getPathValues(test.obj, test.prefix)
		assert.Equal(t, len(test.expectedPvs), len(pvs))
		assert.ElementsMatch(t, test.expectedPvs, pvs)
	}
}

// fuzz test the ordering of our number encoding. Running
// a million combinations gives more confidence about the
// code I found on StackOverflow for byte encoding numbers
// in a way that maintains their ordering.
func Fuzz_fuzzNumberSort(f *testing.F) {
	f.Add(11.0, 12.0)
	f.Add(1.0, 100.0)
	f.Add(-1.0, 100.0)
	f.Add(45.0, 4500000.0)
	f.Add(-4500000.0, 4500000.0)
	f.Add(11.9, 12.0)
	f.Add(-123.23, 123.0)
	f.Add(123.23, 123.25)
	f.Add(123.123, 123.123)
	f.Fuzz(func(t *testing.T, a, b float64) {
		h := pathValueAsKey("a", a)
		l := pathValueAsKey("a", b)
		if a > b {
			assert.True(t, slices.Compare(h, l) > 0)
		} else if a < b {
			assert.True(t, slices.Compare(h, l) < 0)
		} else {
			assert.True(t, slices.Compare(h, l) == 0)
		}
	})
}

func Test_makeStringSort(t *testing.T) {
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
		h := pathValueAsKey("a", test.h)
		l := pathValueAsKey("a", test.l)
		assert.True(t, slices.Compare(h, l) > 0,
			"%v %s !> %s %v", h, test.h, test.l, l)
	}
}

// Check against the CouchDB collation order.
func Test_PVSortTypes(t *testing.T) {
	p := "a.b.c"
	assert.True(t, slices.Compare(pathValueAsKey(p, nil), pathValueAsKey(p, false)) < 0,
		"null should be less than false")
	assert.True(t, slices.Compare(pathValueAsKey(p, false), pathValueAsKey(p, true)) < 0,
		"false should be less than true")
	assert.True(t, slices.Compare(pathValueAsKey(p, true), pathValueAsKey(p, 1234)) < 0,
		"true should be less than number")
	assert.True(t, slices.Compare(pathValueAsKey(p, 1234), pathValueAsKey(p, "1234")) < 0,
		"number should be less than string")
}
