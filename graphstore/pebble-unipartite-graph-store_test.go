package graphstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateEntityId(t *testing.T) {

	testCases := []struct {
		name     string
		entityId string
		expected error
	}{
		{
			name:     "Empty ID",
			entityId: "",
			expected: ErrEmptyEntityId,
		},
		{
			name:     "Entity ID with separator",
			entityId: "A#1",
			expected: ErrEntityIdContainsIllegalCharacter,
		},
		{
			name:     "Numeric entity ID",
			entityId: "1234",
			expected: nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := validateEntityId(testCase.entityId)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestEdgeToPebbleKey(t *testing.T) {

	testCases := []struct {
		src           string
		dst           string
		expectedKey   string
		expectedError error
	}{
		{
			src:           "e1",
			dst:           "e2",
			expectedKey:   edgePrefix + separator + "e1" + separator + "e2",
			expectedError: nil,
		},
		{
			src:           "",
			dst:           "e2",
			expectedKey:   "",
			expectedError: ErrEmptyEntityId,
		},
		{
			src:           "e1",
			dst:           "",
			expectedKey:   "",
			expectedError: ErrEmptyEntityId,
		},
		{
			src:           "e#",
			dst:           "e2",
			expectedKey:   "",
			expectedError: ErrEntityIdContainsIllegalCharacter,
		},
		{
			src:           "e1",
			dst:           "e1",
			expectedKey:   "",
			expectedError: ErrSelfLoop,
		},
	}

	for _, testCase := range testCases {
		actual, err := edgeToPebbleKey(testCase.src, testCase.dst)
		assert.Equal(t, testCase.expectedKey, string(actual))
		assert.ErrorIs(t, err, testCase.expectedError)
	}
}

func TestPebbleKeyToEdge(t *testing.T) {
	testCases := []struct {
		key           string
		src           string
		dst           string
		expectedError error
	}{
		{
			key:           edgePrefix + separator + "e1" + separator + "e2",
			src:           "e1",
			dst:           "e2",
			expectedError: nil,
		},
		{
			key:           edgePrefix + separator + "e#" + separator + "e2",
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           edgePrefix + separator + "e1" + separator + "e#",
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           edgePrefix,
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           edgePrefix + separator,
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           edgePrefix + separator + "e1",
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           edgePrefix + separator + "e1" + separator,
			src:           "",
			dst:           "",
			expectedError: ErrMalformedKey,
		},
	}

	for _, testCase := range testCases {
		src, dst, err := pebbleKeyToEdge([]byte(testCase.key))

		if testCase.expectedError != nil {
			assert.Equal(t, "", src)
			assert.Equal(t, "", dst)
			assert.ErrorIs(t, err, testCase.expectedError)
		} else {
			assert.Equal(t, testCase.src, src)
			assert.Equal(t, testCase.dst, dst)
			assert.NoError(t, err)
		}
	}
}

func TestNodeToPebbleKey(t *testing.T) {
	testCases := []struct {
		node          string
		expectedKey   string
		expectedError error
	}{
		{
			node:          "e1",
			expectedKey:   nodePrefix + separator + "e1",
			expectedError: nil,
		},
		{
			node:          "e#",
			expectedKey:   "",
			expectedError: ErrEntityIdContainsIllegalCharacter,
		},
		{
			node:          "",
			expectedKey:   "",
			expectedError: ErrEmptyEntityId,
		},
	}

	for _, testCase := range testCases {
		actual, err := nodeToPebbleKey(testCase.node)
		assert.Equal(t, testCase.expectedKey, string(actual))
		assert.ErrorIs(t, testCase.expectedError, err)
	}
}

func TestPebbleKeyToNode(t *testing.T) {
	testCases := []struct {
		key           string
		expected      string
		expectedError error
	}{
		{
			key:           nodePrefix + separator + "e1",
			expected:      "e1",
			expectedError: nil,
		},
		{
			key:           nodePrefix,
			expected:      "",
			expectedError: ErrMalformedKey,
		},
		{
			key:           nodePrefix + separator,
			expected:      "",
			expectedError: ErrEmptyEntityId,
		},
		{
			key:           nodePrefix + separator + separator,
			expected:      "",
			expectedError: ErrMalformedKey,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("Test %v", idx), func(t *testing.T) {
			actual, err := pebbleKeyToNode([]byte(testCase.key))
			assert.Equal(t, testCase.expected, actual)
			assert.ErrorIs(t, err, testCase.expectedError)
		})
	}
}

func createTempPebbleFolder(t testing.TB) string {
	dir, err := ioutil.TempDir("", "pebble")
	assert.NoError(t, err)
	return dir
}

func deleteTempPebbleFolder(t *testing.T, folder string) {
	assert.NoError(t, os.RemoveAll(folder))
}

func newUnipartitePebbleStore(t testing.TB) *PebbleUnipartiteGraphStore {
	folder := createTempPebbleFolder(t)
	store, err := NewPebbleUnipartiteGraphStore(folder)
	assert.NoError(t, err)
	return store
}

func cleanUpUnipartitePebbleStore(t testing.TB, store *PebbleUnipartiteGraphStore) {
	assert.NoError(t, store.Destroy())
}

func BenchmarkLoadPebbleUnipartiteGraph(b *testing.B) {
	graph := newUnipartitePebbleStore(b)
	defer cleanUpUnipartitePebbleStore(b, graph)

	maxNumEntities := 100
	numConnections := 400

	// Randomly generate edges to load
	edges := randomEdges(maxNumEntities, numConnections)
	loadEdges(b, graph, edges)

	for i := 0; i < b.N; i++ {

		// Clean the store
		b.StopTimer()
		graph.Clear()
		b.StartTimer()

		loadEdgesConcurrently(b, graph, edges)
	}
}
