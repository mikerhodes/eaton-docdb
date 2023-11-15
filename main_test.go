package main

import (
	"encoding/binary"
	"math"
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

func makePVS(path, value string) []byte {
	pv := []byte(path)
	pv = append(pv, 0)
	pv = append(pv, JSONTagString)
	pv = append(pv, []byte(value)...)
	return pv
}

func makePVI(path string, value int) []byte {
	pv := []byte(path)
	pv = append(pv, 0)
	pv = append(pv, JSONTagNumber)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(float64(value)))
	pv = append(pv, buf[:]...)
	return pv
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
// TODO move makePVS/I into main.go, call from getPathValueKey, so
//    can call from here, then can be sure we use same key for ordering
//    tests and production code.
