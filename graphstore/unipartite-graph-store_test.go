package graphstore

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func SelfConnection(t *testing.T, g UnipartiteGraphStore) {
	g.Clear()
	assert.Error(t, g.AddDirected("a", "a"))
	assert.Error(t, g.AddUndirected("a", "a"))
}

type connection struct {
	source       string
	destinations []string
}

// checkConnections in a unipartite graph.
func checkConnections(t *testing.T, g UnipartiteGraphStore, conns []connection) {
	for _, conn := range conns {
		expected := set.NewPopulatedSet(conn.destinations...)
		actual, err := g.EntityIdsAdjacentTo(conn.source)
		assert.NoError(t, err)
		assert.True(t, expected.Equal(actual))
	}
}

// SimpleGraph1 with the structure:
//
//    A--B
func SimpleGraph1(t *testing.T, g UnipartiteGraphStore) {
	g.Clear()
	assert.NoError(t, g.AddUndirected("A", "B"))

	hasA, err := g.HasEntity("A")
	assert.NoError(t, err)
	assert.True(t, hasA)

	hasB, err := g.HasEntity("B")
	assert.NoError(t, err)
	assert.True(t, hasB)

	hasC, err := g.HasEntity("C")
	assert.NoError(t, err)
	assert.False(t, hasC)

	// Check the entity IDs
	expectedEntityIds := set.NewPopulatedSet("A", "B")
	actualEntityIds, err := g.EntityIds()
	assert.NoError(t, err)
	assert.True(t, expectedEntityIds.Equal(actualEntityIds))

	expectedConnections := []connection{
		{
			source:       "A",
			destinations: []string{"B"},
		},
		{
			source:       "B",
			destinations: []string{"A"},
		},
	}

	checkConnections(t, g, expectedConnections)

	// Try to get a vertex that doesn't exist
	_, err = g.EntityIdsAdjacentTo("C")
	assert.Error(t, err)
}

// SimpleGraph2 with the structure:
//
//         A--B----
//         |      |
//   C--D--E--F---G
//         |      |
//         H-------
func SimpleGraph2(t *testing.T, g UnipartiteGraphStore) {
	g.Clear()
	assert.NoError(t, g.AddUndirected("A", "B"))
	assert.NoError(t, g.AddUndirected("A", "E"))
	assert.NoError(t, g.AddUndirected("B", "G"))
	assert.NoError(t, g.AddUndirected("C", "D"))
	assert.NoError(t, g.AddUndirected("D", "E"))
	assert.NoError(t, g.AddUndirected("E", "F"))
	assert.NoError(t, g.AddUndirected("E", "H"))
	assert.NoError(t, g.AddUndirected("F", "G"))
	assert.NoError(t, g.AddUndirected("H", "G"))

	expectedEntityIds := set.NewPopulatedSet("A", "B", "C", "D", "E", "F", "G", "H")
	actualEntityIds, err := g.EntityIds()
	assert.NoError(t, err)
	assert.True(t, expectedEntityIds.Equal(actualEntityIds))

	expectedConnections := []connection{
		{
			source:       "A",
			destinations: []string{"B", "E"},
		},
		{
			source:       "B",
			destinations: []string{"A", "G"},
		},
		{
			source:       "C",
			destinations: []string{"D"},
		},
		{
			source:       "D",
			destinations: []string{"C", "E"},
		},
		{
			source:       "E",
			destinations: []string{"A", "D", "F", "H"},
		},
		{
			source:       "F",
			destinations: []string{"E", "G"},
		},
		{
			source:       "G",
			destinations: []string{"B", "F", "H"},
		},
		{
			source:       "H",
			destinations: []string{"E", "G"},
		},
	}

	checkConnections(t, g, expectedConnections)
}

// EqualGraphs checks situations where two graphs should be equal.
//
// Graph 1 is:
//
//   A--B--C
//
// Graph 2 is:
//   A--B--C--D
//
// Graph 3 is:
//   A--B--C
//   |     |
//   -------
func EqualGraphs(t *testing.T, g1 UnipartiteGraphStore, g2 UnipartiteGraphStore) {

	// Test 1
	g1.Clear()
	g2.Clear()

	// Graph 1
	assert.NoError(t, g1.AddUndirected("A", "B"))
	assert.NoError(t, g1.AddUndirected("B", "C"))

	// Graph 1
	assert.NoError(t, g2.AddUndirected("A", "B"))
	assert.NoError(t, g2.AddUndirected("B", "C"))
	equal, err := UnipartiteGraphStoresEqual(g1, g2)
	assert.NoError(t, err)
	assert.True(t, equal)

	// Mutate graph 1 into graph 3
	assert.NoError(t, g2.AddUndirected("A", "C"))
	equal, err = UnipartiteGraphStoresEqual(g1, g2)
	assert.NoError(t, err)
	assert.False(t, equal)

	// Make graph 2
	g2.Clear()
	assert.NoError(t, g2.AddUndirected("A", "B"))
	assert.NoError(t, g2.AddUndirected("B", "C"))
	assert.NoError(t, g2.AddUndirected("C", "D"))
	equal, err = UnipartiteGraphStoresEqual(g1, g2)
	assert.NoError(t, err)
	assert.False(t, equal)
}

// Connected checks that the vertices are connected as expected.
//
//         A--B
//         |
//   C--D--E
func Connected(t *testing.T, g UnipartiteGraphStore) {

	g.Clear()

	assert.NoError(t, g.AddUndirected("A", "B"))
	assert.NoError(t, g.AddUndirected("A", "E"))
	assert.NoError(t, g.AddUndirected("C", "D"))
	assert.NoError(t, g.AddUndirected("D", "E"))

	testCases := []struct {
		entity1      string
		entity2      string
		expectedEdge bool
	}{
		{
			entity1:      "A",
			entity2:      "B",
			expectedEdge: true,
		},
		{
			entity1:      "B",
			entity2:      "A",
			expectedEdge: true,
		},
		{
			entity1:      "A",
			entity2:      "E",
			expectedEdge: true,
		},
		{
			entity1:      "E",
			entity2:      "A",
			expectedEdge: true,
		},
		{
			entity1:      "A",
			entity2:      "D",
			expectedEdge: false,
		},
		{
			entity1:      "D",
			entity2:      "A",
			expectedEdge: false,
		},
	}

	for _, testCase := range testCases {
		actual, err := g.EdgeExists(testCase.entity1, testCase.entity2)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedEdge, actual)
	}
}

func TestInMemory(t *testing.T) {
	g := NewInMemoryUnipartiteGraphStore()

	SelfConnection(t, g)
	SimpleGraph1(t, g)
	SimpleGraph2(t, g)

	Connected(t, g)

	g2 := NewInMemoryUnipartiteGraphStore()
	EqualGraphs(t, g, g2)
}
