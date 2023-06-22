package graphstore

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func checkSelfConnection(t *testing.T, g UnipartiteGraphStore) {
	g.Clear()
	assert.Error(t, g.AddDirected("a", "a"))
	assert.Error(t, g.AddUndirected("a", "a"))
}

type connection struct {
	source       string
	destinations []string
}

// checkConnections in a unipartite graph.
func checkConnections(t testing.TB, g UnipartiteGraphStore, conns []connection) {
	for _, conn := range conns {
		expected := set.NewPopulatedSet(conn.destinations...)
		actual, err := g.EntityIdsAdjacentTo(conn.source)
		assert.NoError(t, err)
		if !expected.Equal(actual) {
			fmt.Printf("Source: %v, expected dsts: %v, actual dsts: %v\n", conn, expected.String(), actual.String())
		}
		assert.True(t, expected.Equal(actual))
	}
}

// simpleGraph1 with the structure:
//
//	A--B
func simpleGraph1(t *testing.T, g UnipartiteGraphStore) {
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

// simpleGraph2 with the structure:
//
//	      A--B----
//	      |      |
//	C--D--E--F---G
//	      |      |
//	      H-------
func simpleGraph2(t *testing.T, g UnipartiteGraphStore) {
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

// equalGraphs checks situations where two graphs should be equal.
//
// Graph 1 is:
//
//	A--B--C
//
// Graph 2 is:
//
//	A--B--C--D
//
// Graph 3 is:
//
//	A--B--C
//	|     |
//	-------
func equalGraphs(t *testing.T, g1 UnipartiteGraphStore, g2 UnipartiteGraphStore) {

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

// checkConnected checks that the vertices are connected as expected.
//
//	      A--B
//	      |
//	C--D--E
func checkConnected(t *testing.T, g UnipartiteGraphStore) {

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

func TestUnipartiteGraphStore(t *testing.T) {

	// Make the in-memory unipartite graph store
	inMemory := NewInMemoryUnipartiteGraphStore()

	// Make the Pebble unipartite graph store
	pebbleGraphStore := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, pebbleGraphStore)

	graphStores := []UnipartiteGraphStore{
		inMemory,
		pebbleGraphStore,
	}

	for _, gs := range graphStores {

		checkSelfConnection(t, gs)
		simpleGraph1(t, gs)
		simpleGraph2(t, gs)
		checkConnected(t, gs)

		g2 := NewInMemoryUnipartiteGraphStore()
		equalGraphs(t, gs, g2)
	}
}

func TestCalcUnipartiteStats(t *testing.T) {

	// Make the in-memory unipartite graph store
	inMemory := NewInMemoryUnipartiteGraphStore()

	// Make the Pebble unipartite graph store
	pebbleGraphStore := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, pebbleGraphStore)

	graphStores := []UnipartiteGraphStore{
		inMemory,
		pebbleGraphStore,
	}

	for _, gs := range graphStores {

		stats, err := CalcUnipartiteStats(gs)
		assert.NoError(t, err)
		assert.Equal(t, UnipartiteStats{
			NumberOfEntities: 0,
		}, stats)

		assert.NoError(t, gs.AddUndirected("e-1", "e-2"))
		stats, err = CalcUnipartiteStats(gs)
		assert.NoError(t, err)
		assert.Equal(t, UnipartiteStats{
			NumberOfEntities: 2,
		}, stats)

		assert.NoError(t, gs.AddEntity("e-3"))
		stats, err = CalcUnipartiteStats(gs)
		assert.NoError(t, err)
		assert.Equal(t, UnipartiteStats{
			NumberOfEntities: 3,
		}, stats)

		assert.NoError(t, gs.AddUndirected("e-3", "e-4"))
		stats, err = CalcUnipartiteStats(gs)
		assert.NoError(t, err)
		assert.Equal(t, UnipartiteStats{
			NumberOfEntities: 4,
		}, stats)
	}
}

// TestUnipartiteConcurrency tests whether the result of concurrent loading of the unipartite graph
// provides consistent results. The graph that is loaded is the following:
//
//	[e-1] ----------- --- [e-2] -------------- [e-3]
//	  |                     |                     |
//	  |--------[e-4]--------|                     |
//	             |                                |
//	             |                                |
//	             |------- [e-5] -------------- [e-6]
func TestUnipartiteConcurrency(t *testing.T) {

	// Make the in-memory unipartite graph stores
	inMemoryNoConcurrency := NewInMemoryUnipartiteGraphStore()
	inMemoryWithConcurrency := NewInMemoryUnipartiteGraphStore()

	// Make the Pebble unipartite graph stores
	pebbleGraphStoreNoConcurrency := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, pebbleGraphStoreNoConcurrency)

	pebbleGraphStoreWithConcurrency := newUnipartitePebbleStore(t)
	defer cleanUpUnipartitePebbleStore(t, pebbleGraphStoreWithConcurrency)

	testCases := []struct {
		description               string
		unipartiteNoConcurrency   UnipartiteGraphStore
		unipartiteWithConcurrency UnipartiteGraphStore
	}{
		{
			description:               "in-memory",
			unipartiteNoConcurrency:   inMemoryNoConcurrency,
			unipartiteWithConcurrency: inMemoryWithConcurrency,
		},
		{
			description:               "pebble",
			unipartiteNoConcurrency:   pebbleGraphStoreNoConcurrency,
			unipartiteWithConcurrency: pebbleGraphStoreWithConcurrency,
		},
	}

	// Define edges to load
	edgeDefinitions := []string{
		"e1 - e2",
		"e2 - e3",
		"e1 - e4",
		"e2 - e4",
		"e4 - e5",
		"e5 - e6",
		"e6 - e3",
	}

	edges, err := edgeStringsToEdges(edgeDefinitions, "-")
	assert.NoError(t, err)

	// Shuffle the edges
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(edges), func(i, j int) {
		edges[i], edges[j] = edges[j], edges[i]
	})

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			// Load the unipartite graph store without concurrency
			for _, edge := range edges {
				err := testCase.unipartiteNoConcurrency.AddUndirected(edge.V1, edge.V2)
				assert.NoError(t, err)
			}

			// Concurrently load the unipartite graph store
			midPoint := int(math.Floor(float64(len(edges)) / 2.0))
			edges1 := edges[:midPoint]
			edges2 := edges[midPoint:]

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for _, edge := range edges1 {
					testCase.unipartiteWithConcurrency.AddUndirected(edge.V1, edge.V2)
				}
				wg.Done()
			}()

			wg.Add(1)
			go func() {
				for _, edge := range edges2 {
					testCase.unipartiteWithConcurrency.AddUndirected(edge.V1, edge.V2)
				}
				wg.Done()
			}()

			wg.Wait()

			// Check the result is as expected
			equal, err := UnipartiteGraphStoresEqual(testCase.unipartiteNoConcurrency,
				testCase.unipartiteWithConcurrency)
			assert.NoError(t, err)
			assert.True(t, equal)
		})
	}
}
