package bfs

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

// buildTestGraph shown below:
//
//   1 --- 2 --- 3                   6 (isolated node)
//         |     |
//         4 --- 5
//
//               9 --- 10
//               |   /             16
//               8  /             /
//               | /             /
//        11 --- 7 --- 12 --- 13 --- 15
//                            |
//                            14
func buildTestGraph(t *testing.T, graph graphstore.UnipartiteGraphStore) {
	edgeList := []graphstore.Edge{
		// Connected component #1
		{
			V1: "1",
			V2: "2",
		},
		{
			V1: "2",
			V2: "3",
		},
		{
			V1: "2",
			V2: "4",
		},
		{
			V1: "3",
			V2: "5",
		},
		{
			V1: "4",
			V2: "5",
		},

		// Connected component #2
		{
			V1: "9",
			V2: "10",
		},
		{
			V1: "8",
			V2: "9",
		},
		{
			V1: "7",
			V2: "8",
		},
		{
			V1: "7",
			V2: "10",
		},
		{
			V1: "7",
			V2: "11",
		},
		{
			V1: "7",
			V2: "12",
		},
		{
			V1: "12",
			V2: "13",
		},
		{
			V1: "13",
			V2: "14",
		},
		{
			V1: "13",
			V2: "15",
		},
		{
			V1: "13",
			V2: "16",
		},
	}

	// Populate the graph
	assert.NoError(t, graph.BuildFromEdgeList(edgeList))
	graph.AddEntity("6")
}

// TestReachableVertices checks whether vertices are reachable within a set
// number of steps from a starting vertex.
func TestReachableVertices(t *testing.T) {

	// Create the test graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Test cases
	testCases := []struct {
		root     string
		maxDepth int
		expected *set.Set[string]
	}{
		// Small connected component
		{
			root:     "1",
			maxDepth: 0,
			expected: set.NewPopulatedSet("1"),
		},
		{
			root:     "1",
			maxDepth: 1,
			expected: set.NewPopulatedSet("1", "2"),
		},
		{
			root:     "1",
			maxDepth: 2,
			expected: set.NewPopulatedSet("1", "2", "3", "4"),
		},
		{
			root:     "1",
			maxDepth: 3,
			expected: set.NewPopulatedSet("1", "2", "3", "4", "5"),
		},
		{
			root:     "1",
			maxDepth: 10,
			expected: set.NewPopulatedSet("1", "2", "3", "4", "5"),
		},
		// Isolated node
		{
			root:     "6",
			maxDepth: 0,
			expected: set.NewPopulatedSet("6"),
		},
		{
			root:     "6",
			maxDepth: 1,
			expected: set.NewPopulatedSet("6"),
		},
		// Large connected component
		{
			root:     "7",
			maxDepth: 0,
			expected: set.NewPopulatedSet("7"),
		},
		{
			root:     "7",
			maxDepth: 1,
			expected: set.NewPopulatedSet("7", "11", "8", "10", "12"),
		},
		{
			root:     "7",
			maxDepth: 2,
			expected: set.NewPopulatedSet("7", "11", "8", "10", "12", "9", "13"),
		},
		{
			root:     "7",
			maxDepth: 3,
			expected: set.NewPopulatedSet("7", "11", "8", "10", "12", "9", "13", "14", "15", "16"),
		},
	}

	for _, testCase := range testCases {
		actual, err := ReachableVertices(graph, testCase.root, testCase.maxDepth)
		assert.NoError(t, err)
		assert.True(t, testCase.expected.Equal(actual))
	}

}

// TestReachableVerticesErrorCases checks the preconditions for ReachableVertices.
func TestReachableVerticesErrorCases(t *testing.T) {

	// Create the test graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Entity not found
	_, err := ReachableVertices(graph, "A", 3)
	assert.Error(t, err)

	// Invalid depth
	_, err = ReachableVertices(graph, "1", -1)
	assert.Error(t, err)
}
