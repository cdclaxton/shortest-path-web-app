package bfs

import (
	"fmt"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestNetworkConnections(t *testing.T) {

	n := NewNetworkConnections(2)
	n.AddPaths("A", "set-A", "B", "set-B", []Path{NewPath("A", "B", "C")})
	n.AddPaths("A", "set-A2", "C", "set-C", []Path{NewPath("A", "D", "C")})
	n.AddPaths("E", "set-E", "B", "set-B", []Path{NewPath("E", "B"), NewPath("E", "A", "B")})

	expected := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"A": set.NewPopulatedSet("set-A", "set-A2"),
			"B": set.NewPopulatedSet("set-B"),
			"C": set.NewPopulatedSet("set-C"),
			"E": set.NewPopulatedSet("set-E"),
		},
		Connections: map[string]map[string][]Path{
			"A": {
				"B": []Path{NewPath("A", "B", "C")},
				"C": []Path{NewPath("A", "D", "C")},
			},
			"E": {
				"B": []Path{NewPath("E", "B"), NewPath("E", "A", "B")},
			},
		},
		MaxHops: 2,
	}

	assert.True(t, expected.Equal(n))
}

// Test findAllPathsWithResilience() using the graph:
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
func TestFindAllPathsWithResilience(t *testing.T) {

	// Construct the unipartite graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Construct a new path finder component
	pathFinder := NewPathFinder(graph)

	testCases := []struct {
		root          string
		goal          string
		maxHops       int
		expectedPaths []Path
	}{
		{
			root:          "1",
			goal:          "3",
			maxHops:       1,
			expectedPaths: []Path{},
		},
		{
			root:          "1",
			goal:          "3",
			maxHops:       2,
			expectedPaths: []Path{NewPath("1", "2", "3")},
		},
		{
			root:          "1",
			goal:          "5",
			maxHops:       3,
			expectedPaths: []Path{NewPath("1", "2", "3", "5"), NewPath("1", "2", "4", "5")},
		},
		{
			root:          "1",
			goal:          "NON", // doesn't exist
			maxHops:       3,
			expectedPaths: []Path{},
		},
	}

	for _, testCase := range testCases {
		actualPaths, err := pathFinder.findAllPathsWithResilience(testCase.root, testCase.goal,
			testCase.maxHops)
		assert.NoError(t, err)
		assert.True(t, PathsEqual(testCase.expectedPaths, actualPaths))
	}
}

// Test pathsBetweenEntitySets() using the graph:
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
func TestPathsBetweenEntitySets(t *testing.T) {

	// Construct the unipartite graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Construct a new path finder component
	pathFinder := NewPathFinder(graph)

	entitySet1 := job.EntitySet{
		EntityIds: []string{"1", "3", "9", "10"},
		Name:      "Set-1",
	}

	entitySet2 := job.EntitySet{
		EntityIds: []string{"1", "2", "7", "NON"},
		Name:      "Set-2",
	}

	// Find the paths
	actualConnections := NewNetworkConnections(3)
	err := pathFinder.pathsBetweenEntitySets(entitySet1, entitySet2, actualConnections)
	assert.NoError(t, err)

	// Check the connections
	expectedConnections := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"1":   set.NewPopulatedSet("Set-1", "Set-2"),
			"2":   set.NewPopulatedSet("Set-2"),
			"3":   set.NewPopulatedSet("Set-1"),
			"7":   set.NewPopulatedSet("Set-2"),
			"9":   set.NewPopulatedSet("Set-1"),
			"10":  set.NewPopulatedSet("Set-1"),
			"NON": set.NewPopulatedSet("Set-2"),
		},
		Connections: map[string]map[string][]Path{
			"1": {"2": []Path{
				NewPath("1", "2")}},
			"3": {
				"2": []Path{
					NewPath("3", "2"),
					NewPath("3", "5", "4", "2")},
				"1": []Path{
					NewPath("3", "2", "1")},
			},
			"9": {"7": []Path{
				NewPath("9", "8", "7"),
				NewPath("9", "10", "7")},
			},
			"10": {"7": []Path{
				NewPath("10", "9", "8", "7"),
				NewPath("10", "7")}},
		},
		MaxHops: 3,
	}

	assert.True(t, expectedConnections.Equal(actualConnections))
}

// Test pathsBetweenAllEntitySets() using the graph:
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
func TestPathsBetweenAllEntitySets(t *testing.T) {

	// Construct the unipartite graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Construct a new path finder component
	pathFinder := NewPathFinder(graph)

	entitySets := []job.EntitySet{
		{
			EntityIds: []string{"1", "3", "9"},
			Name:      "Set-1",
		},
		{
			EntityIds: []string{"1", "2", "7", "A"},
			Name:      "Set-2",
		},
		{
			EntityIds: []string{"1", "2", "12", "B"},
			Name:      "Set-3",
		},
	}

	// Find the paths
	actualConnections := NewNetworkConnections(3)
	err := pathFinder.pathsBetweenAllEntitySets(entitySets, actualConnections)
	assert.NoError(t, err)

	// Check the connections
	expectedConnections := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"1":  set.NewPopulatedSet("Set-1", "Set-2", "Set-3"),
			"2":  set.NewPopulatedSet("Set-2", "Set-3"),
			"3":  set.NewPopulatedSet("Set-1"),
			"7":  set.NewPopulatedSet("Set-2"),
			"9":  set.NewPopulatedSet("Set-1"),
			"12": set.NewPopulatedSet("Set-3"),
			"A":  set.NewPopulatedSet("Set-2"),
			"B":  set.NewPopulatedSet("Set-3"),
		},
		Connections: map[string]map[string][]Path{
			"1": {
				"2": []Path{NewPath("1", "2")},
			},
			"3": {
				"1": []Path{NewPath("3", "2", "1")},
				"2": []Path{NewPath("3", "2"), NewPath("3", "5", "4", "2")},
			},
			"7": {
				"12": []Path{NewPath("7", "12")},
			},
			"9": {
				"7":  []Path{NewPath("9", "8", "7"), NewPath("9", "10", "7")},
				"12": []Path{NewPath("9", "10", "7", "12"), NewPath("9", "8", "7", "12")},
			},
		},
		MaxHops: 3,
	}

	assert.True(t, expectedConnections.Equal(actualConnections))
}

// Test FindPaths() using the graph:
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
func TestFindPathsOneEntitySet(t *testing.T) {
	// Construct the unipartite graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Construct a new path finder component
	pathFinder := NewPathFinder(graph)

	entitySets := []job.EntitySet{
		{
			EntityIds: []string{"1", "3", "9", "10", "A"},
			Name:      "Set-1",
		},
	}

	actualConnections, err := pathFinder.FindPaths(entitySets, 3)
	assert.NoError(t, err)

	// Check the connections
	expectedConnections := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"1":  set.NewPopulatedSet("Set-1"),
			"3":  set.NewPopulatedSet("Set-1"),
			"9":  set.NewPopulatedSet("Set-1"),
			"10": set.NewPopulatedSet("Set-1"),
			"A":  set.NewPopulatedSet("Set-1"),
		},
		Connections: map[string]map[string][]Path{
			"1": {
				"3": []Path{NewPath("1", "2", "3")},
			},
			"9": {
				"10": []Path{NewPath("9", "10"), NewPath("9", "8", "7", "10")},
			},
		},
		MaxHops: 3,
	}

	assert.True(t, expectedConnections.Equal(actualConnections))
}

// Test FindPaths() using the graph:
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
func TestFindPathsTwoEntitySets(t *testing.T) {
	// Construct the unipartite graph
	graph := graphstore.NewInMemoryUnipartiteGraphStore()
	buildTestGraph(t, graph)

	// Construct a new path finder component
	pathFinder := NewPathFinder(graph)

	entitySets := []job.EntitySet{
		{
			EntityIds: []string{"1", "3", "9", "10", "A"},
			Name:      "Set-1",
		},
		{
			EntityIds: []string{"1", "11", "12", "B"},
			Name:      "Set-2",
		},
	}

	actualConnections, err := pathFinder.FindPaths(entitySets, 3)
	assert.NoError(t, err)

	// Check the connections
	expectedConnections := NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"1":  set.NewPopulatedSet("Set-1", "Set-2"),
			"3":  set.NewPopulatedSet("Set-1"),
			"9":  set.NewPopulatedSet("Set-1"),
			"10": set.NewPopulatedSet("Set-1"),
			"11": set.NewPopulatedSet("Set-2"),
			"12": set.NewPopulatedSet("Set-2"),
			"A":  set.NewPopulatedSet("Set-1"),
			"B":  set.NewPopulatedSet("Set-2"),
		},
		Connections: map[string]map[string][]Path{
			"3": {
				"1": []Path{NewPath("3", "2", "1")},
			},
			"9": {
				"11": []Path{NewPath("9", "8", "7", "11"), NewPath("9", "10", "7", "11")},
				"12": []Path{NewPath("9", "10", "7", "12"), NewPath("9", "8", "7", "12")},
			},
			"10": {
				"12": []Path{NewPath("10", "7", "12")},
				"11": []Path{NewPath("10", "7", "11")},
			},
		},
		MaxHops: 3,
	}

	fmt.Println(actualConnections)
	fmt.Println(&expectedConnections)

	assert.True(t, expectedConnections.Equal(actualConnections))
}
