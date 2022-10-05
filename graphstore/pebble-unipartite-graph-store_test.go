package graphstore

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestEntityIdToPebbleKey(t *testing.T) {
	testCases := []string{"", "A", "AB", "ABC"}

	for _, id := range testCases {
		// Test the round trip from an id to a Pebble key and back to an id
		assert.Equal(t, id, pebbleKeyToEntityId(entityIdToPebbleKey(id)))
	}
}

func TestEntityIdsToPebbleValue(t *testing.T) {
	testCases := []*set.Set[string]{
		set.NewPopulatedSet[string](),
		set.NewPopulatedSet("A"),
		set.NewPopulatedSet("A", "B"),
		set.NewPopulatedSet("A", "B", "C"),
	}

	for _, testCase := range testCases {
		value, err := entityIdsToPebbleValue(testCase)
		assert.NoError(t, err)

		s, err := pebbleValueToEntityIds(value)
		assert.NoError(t, err)
		assert.True(t, testCase.Equal(s))
	}
}

func createTempPebbleFolder(t *testing.T) string {
	dir, err := ioutil.TempDir("", "pebble")
	assert.NoError(t, err)
	return dir
}

func deleteTempPebbleFolder(t *testing.T, folder string) {
	assert.NoError(t, os.RemoveAll(folder))
}

func newUnipartitePebbleStore(t *testing.T) *PebbleUnipartiteGraphStore {
	folder := createTempPebbleFolder(t)
	store, err := NewPebbleUnipartiteGraphStore(folder)
	assert.NoError(t, err)
	return store
}

func cleanUpUnipartitePebbleStore(t *testing.T, store *PebbleUnipartiteGraphStore) {
	assert.NoError(t, store.Close())
	assert.NoError(t, os.RemoveAll(store.folder))
}

func TestSetAndGetFromStore(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	keyValues := map[string]*set.Set[string]{
		"src-0": set.NewSet[string](),
		"src-1": set.NewPopulatedSet("dst-1"),
		"src-2": set.NewPopulatedSet("dst-1", "dst-2"),
	}

	// Store the key-value pairs
	for src, dsts := range keyValues {
		assert.NoError(t, store.setSrcToDsts(src, dsts))
	}

	// Try to get a key that doesn't exist
	s, found, err := store.dstEntityIds("unknown")
	assert.Nil(t, s)
	assert.False(t, found)
	assert.NoError(t, err)

	// Try to get a key that does exist
	for src, dsts := range keyValues {
		s, found, err = store.dstEntityIds(src)
		assert.True(t, found)
		assert.NoError(t, err)
		assert.True(t, s.Equal(dsts))
	}
}

func TestAddEntity(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Non-existent entity
	found, err := store.HasEntity("e-1")
	assert.NoError(t, err)
	assert.False(t, found)

	// Add an entity and check whether it exists in the database
	assert.NoError(t, store.AddEntity("e-1"))

	found, err = store.HasEntity("e-1")
	assert.NoError(t, err)
	assert.True(t, found)

	// Add the same entity again
	assert.NoError(t, store.AddEntity("e-1"))

	found, err = store.HasEntity("e-1")
	assert.NoError(t, err)
	assert.True(t, found)

	// Add another entity
	assert.NoError(t, store.AddEntity("e-2"))

	found, err = store.HasEntity("e-1")
	assert.NoError(t, err)
	assert.True(t, found)

	found, err = store.HasEntity("e-2")
	assert.NoError(t, err)
	assert.True(t, found)
}

func destinationsCorrect(t *testing.T, store *PebbleUnipartiteGraphStore,
	src string, dsts []string) {

	actual, found, err := store.dstEntityIds(src)

	assert.NoError(t, err)
	assert.True(t, found)

	expected := set.NewPopulatedSet(dsts...)
	assert.True(t, expected.Equal(actual))
}

func TestAddDirected(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Non-existent entity
	found, err := store.HasEntity("e-1")
	assert.NoError(t, err)
	assert.False(t, found)

	// Add a self-link
	assert.Error(t, store.AddDirected("e-1", "e-1"))

	// Add a directed link
	assert.NoError(t, store.AddDirected("e-1", "e-2"))
	destinationsCorrect(t, store, "e-1", []string{"e-2"})

	// Add another link from a pre-existing source
	assert.NoError(t, store.AddDirected("e-1", "e-3"))
	destinationsCorrect(t, store, "e-1", []string{"e-2", "e-3"})
}

func TestAddUndirected(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Add e-1 <--> e-2 connection
	assert.NoError(t, store.AddUndirected("e-1", "e-2"))
	destinationsCorrect(t, store, "e-1", []string{"e-2"})
	destinationsCorrect(t, store, "e-2", []string{"e-1"})

	ids, err := store.EntityIds()
	assert.NoError(t, err)
	assert.True(t, set.NewPopulatedSet("e-1", "e-2").Equal(ids))

	// Add e-1 <--> e-3 connection. This yields the graph:
	//
	//   e-1 --- e-2
	//    |
	//   e-3
	assert.NoError(t, store.AddUndirected("e-1", "e-3"))
	destinationsCorrect(t, store, "e-1", []string{"e-2", "e-3"})
	destinationsCorrect(t, store, "e-2", []string{"e-1"})
	destinationsCorrect(t, store, "e-3", []string{"e-1"})

	ids, err = store.EntityIds()
	assert.NoError(t, err)
	assert.True(t, set.NewPopulatedSet("e-1", "e-2", "e-3").Equal(ids))
}

func TestBuildFromEdgeList(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Build the graph:
	//
	//   e-1 --- e-2 --- e-3
	//    |      /
	//    |    /
	//   e-4 -
	edges := []Edge{
		{
			V1: "e-1",
			V2: "e-2",
		},
		{
			V1: "e-2",
			V2: "e-3",
		},
		{
			V1: "e-1",
			V2: "e-4",
		},
		{
			V1: "e-4",
			V2: "e-2",
		},
	}

	assert.NoError(t, BuildFromEdgeList(store, edges))

	destinationsCorrect(t, store, "e-1", []string{"e-2", "e-4"})
	destinationsCorrect(t, store, "e-2", []string{"e-1", "e-4", "e-3"})
	destinationsCorrect(t, store, "e-3", []string{"e-2"})
	destinationsCorrect(t, store, "e-4", []string{"e-1", "e-2"})

	ids, err := store.EntityIds()
	assert.NoError(t, err)
	assert.True(t, set.NewPopulatedSet("e-1", "e-2", "e-3", "e-4").Equal(ids))
}

func TestClear(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Build the graph:
	//
	//   e-1 --- e-2 --- e-3
	edges := []Edge{
		{
			V1: "e-1",
			V2: "e-2",
		},
		{
			V1: "e-2",
			V2: "e-3",
		},
	}

	assert.NoError(t, BuildFromEdgeList(store, edges))

	ids, err := store.EntityIds()
	assert.NoError(t, err)
	assert.True(t, set.NewPopulatedSet("e-1", "e-2", "e-3").Equal(ids))

	// Clear the graph
	assert.NoError(t, store.Clear())
	ids, err = store.EntityIds()
	assert.NoError(t, err)
	assert.Equal(t, 0, ids.Len())
}

func TestEdgeExists(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Build the graph:
	//
	//   e-1 --- e-2 --- e-3
	edges := []Edge{
		{
			V1: "e-1",
			V2: "e-2",
		},
		{
			V1: "e-2",
			V2: "e-3",
		},
	}

	assert.NoError(t, BuildFromEdgeList(store, edges))

	testCases := []struct {
		src        string
		dst        string
		linkExists bool
	}{
		{
			src:        "e-1",
			dst:        "e-2",
			linkExists: true,
		},
		{
			src:        "e-1",
			dst:        "e-3",
			linkExists: false,
		},
		{
			src:        "e-2",
			dst:        "e-3",
			linkExists: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := store.EdgeExists(testCase.src, testCase.dst)
		assert.NoError(t, err)
		assert.Equal(t, testCase.linkExists, actual)
	}
}

func TestEntityIdsAdjacentTo(t *testing.T) {
	store := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, store)

	// Build the graph:
	//
	//   e-1 --- e-2 --- e-3
	//    |      /
	//    |    /
	//   e-4 -
	edges := []Edge{
		{
			V1: "e-1",
			V2: "e-2",
		},
		{
			V1: "e-2",
			V2: "e-3",
		},
		{
			V1: "e-1",
			V2: "e-4",
		},
		{
			V1: "e-4",
			V2: "e-2",
		},
	}

	assert.NoError(t, BuildFromEdgeList(store, edges))

	// Test cases
	testCases := []struct {
		entity           string
		expectedEntities *set.Set[string]
		errorExpected    bool
	}{
		{
			entity:           "e-1",
			expectedEntities: set.NewPopulatedSet("e-2", "e-4"),
			errorExpected:    false,
		},
		{
			entity:           "Unknown", // Entity doesn't exist
			expectedEntities: nil,
			errorExpected:    true,
		},
		{
			entity:           "e-2",
			expectedEntities: set.NewPopulatedSet("e-1", "e-3", "e-4"),
			errorExpected:    false,
		},
		{
			entity:           "e-3",
			expectedEntities: set.NewPopulatedSet("e-2"),
			errorExpected:    false,
		},
		{
			entity:           "e-4",
			expectedEntities: set.NewPopulatedSet("e-1", "e-2"),
			errorExpected:    false,
		},
	}

	for _, testCase := range testCases {
		actual, err := store.EntityIdsAdjacentTo(testCase.entity)

		if testCase.errorExpected {
			assert.Error(t, err)
			assert.Nil(t, actual)
		} else {
			assert.NoError(t, err)
			assert.True(t, testCase.expectedEntities.Equal(actual))
		}
	}
}
