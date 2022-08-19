package bfs

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/stretchr/testify/assert"
)

func TestPathsEqual(t *testing.T) {
	path1 := NewPath("A", "B")
	path2 := NewPath("A", "B", "C")
	path3 := NewPath("A", "B", "D")

	testCases := []struct {
		paths1   []Path
		paths2   []Path
		expected bool
	}{
		{
			paths1:   []Path{path1},
			paths2:   []Path{path1},
			expected: true,
		},
		{
			paths1:   []Path{path1, path2},
			paths2:   []Path{path2, path1},
			expected: true,
		},
		{
			paths1:   []Path{path1, path2},
			paths2:   []Path{path1, path1}, // Same path twice
			expected: false,
		},
		{
			paths1:   []Path{path1, path2, path3},
			paths2:   []Path{path2, path3, path1},
			expected: true,
		},
		{
			paths1:   []Path{path1, path2, path3},
			paths2:   []Path{path3, path2, path1}, // Different order to above
			expected: true,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, PathsEqual(testCase.paths1, testCase.paths2))
	}

}

type PathsTestCase struct {
	root          string // Starting vertex
	goal          string // Ending vertex
	maxDepth      int    // Maximum number of hops
	expectedPaths []Path // Expected paths (routes) from root to goal
}

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
func TestAllPaths(t *testing.T) {

	// Create the test graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Test cases
	testCases := []PathsTestCase{
		{
			root:          "1",
			goal:          "2",
			maxDepth:      0,
			expectedPaths: []Path{},
		},
		{
			root:          "1",
			goal:          "2",
			maxDepth:      1,
			expectedPaths: []Path{NewPath("1", "2")},
		},
		{
			root:          "1",
			goal:          "2",
			maxDepth:      2,
			expectedPaths: []Path{NewPath("1", "2")},
		},
		{
			root:          "1",
			goal:          "4",
			maxDepth:      2,
			expectedPaths: []Path{NewPath("1", "2", "4")},
		},
		{
			root:          "1",
			goal:          "4",
			maxDepth:      3,
			expectedPaths: []Path{NewPath("1", "2", "4")},
		},
		{
			root:     "1",
			goal:     "4",
			maxDepth: 4,
			expectedPaths: []Path{
				NewPath("1", "2", "4"),
				NewPath("1", "2", "3", "5", "4"),
			},
		},
		{
			root:     "1",
			goal:     "4",
			maxDepth: 5,
			expectedPaths: []Path{
				NewPath("1", "2", "4"),
				NewPath("1", "2", "3", "5", "4"),
			},
		},
		{
			root:          "9",
			goal:          "13",
			maxDepth:      1,
			expectedPaths: []Path{},
		},
		{
			root:          "9",
			goal:          "13",
			maxDepth:      2,
			expectedPaths: []Path{},
		},
		{
			root:          "9",
			goal:          "13",
			maxDepth:      3,
			expectedPaths: []Path{},
		},
		{
			root:     "9",
			goal:     "13",
			maxDepth: 4,
			expectedPaths: []Path{
				NewPath("9", "8", "7", "12", "13"),
				NewPath("9", "10", "7", "12", "13"),
			},
		},
		{
			root:     "9",
			goal:     "15",
			maxDepth: 5,
			expectedPaths: []Path{
				NewPath("9", "10", "7", "12", "13", "15"),
				NewPath("9", "8", "7", "12", "13", "15"),
			},
		},
		{
			root:          "1",
			goal:          "6",
			maxDepth:      5,
			expectedPaths: []Path{},
		},
	}

	for _, testCase := range testCases {
		actualPaths, err := AllPaths(graph, testCase.root, testCase.goal, testCase.maxDepth)
		assert.NoError(t, err)
		assert.True(t, PathsEqual(testCase.expectedPaths, actualPaths))
	}

}

func TestStartEndOfPath(t *testing.T) {
	testCases := []struct {
		path  Path
		start string
		end   string
	}{
		{
			path:  NewPath("A"),
			start: "A",
			end:   "A",
		},
		{
			path:  NewPath("A", "B"),
			start: "A",
			end:   "B",
		},
		{
			path:  NewPath("A", "B", "C"),
			start: "A",
			end:   "C",
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.start, testCase.path.Start())
		assert.Equal(t, testCase.end, testCase.path.End())
	}
}
