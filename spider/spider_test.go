package spider

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestNewSpider(t *testing.T) {
	s, err := NewSpider(nil)
	assert.Nil(t, s)
	assert.Equal(t, ErrUnipartiteIsNil, err)
}

// makeTestGraph constructs a unipartite graph for testing.
//
//	7    8               4 --- 5
//	\   /      11              |
//	 \ /       |               15
//	  1 ------ 2 ---- 3       /  \
//	  |        |             16  17
//	  9 ----- 10
//	  |        |
//	 12       14              6
//	  |
//	 13
func makeTestGraph(t *testing.T) graphstore.UnipartiteGraphStore {

	graph := graphstore.NewInMemoryUnipartiteGraphStore()

	// Connected component 1
	assert.NoError(t, graph.AddUndirected("7", "1"))
	assert.NoError(t, graph.AddUndirected("8", "1"))
	assert.NoError(t, graph.AddUndirected("1", "2"))
	assert.NoError(t, graph.AddUndirected("2", "3"))
	assert.NoError(t, graph.AddUndirected("2", "11"))
	assert.NoError(t, graph.AddUndirected("1", "9"))
	assert.NoError(t, graph.AddUndirected("2", "10"))
	assert.NoError(t, graph.AddUndirected("9", "10"))
	assert.NoError(t, graph.AddUndirected("9", "12"))
	assert.NoError(t, graph.AddUndirected("12", "13"))
	assert.NoError(t, graph.AddUndirected("10", "14"))

	// Connected component 2
	assert.NoError(t, graph.AddUndirected("4", "5"))
	assert.NoError(t, graph.AddUndirected("5", "15"))
	assert.NoError(t, graph.AddUndirected("15", "16"))
	assert.NoError(t, graph.AddUndirected("15", "17"))

	// Connected component 3
	assert.NoError(t, graph.AddEntity("6"))

	return graph
}

func TestExecute(t *testing.T) {

	// Unipartite sub-graph for test case 2 and 3
	graph2 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph2.AddEntity("1")

	// Unipartite sub-graph for test case 4
	graph4 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph4.AddUndirected("1", "2")

	// Unipartite sub-graph for test case 5
	graph5 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph5.AddUndirected("1", "2")
	graph5.AddUndirected("1", "7")
	graph5.AddUndirected("1", "8")
	graph5.AddUndirected("1", "9")

	// Unipartite sub-graph for test case 6 (spidering from 1 and 3)
	graph6 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph6.AddUndirected("1", "2")
	graph6.AddUndirected("1", "7")
	graph6.AddUndirected("1", "8")
	graph6.AddUndirected("1", "9")
	graph6.AddUndirected("3", "2")

	// Unipartite sub-graph for test case 7 (spidering from 1 with 2 steps)
	graph7 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph7.AddUndirected("1", "2")  // step 1
	graph7.AddUndirected("1", "7")  // step 1
	graph7.AddUndirected("1", "8")  // step 1
	graph7.AddUndirected("1", "9")  // step 1
	graph7.AddUndirected("2", "11") // step 2
	graph7.AddUndirected("2", "3")  // step 2
	graph7.AddUndirected("2", "10") // step 2
	graph7.AddUndirected("9", "10") // step 2
	graph7.AddUndirected("9", "12") // step 2

	// Unipartite sub-graph for test case 8 (spidering from 1, 4, 6)
	graph8 := graphstore.NewInMemoryUnipartiteGraphStore()
	graph8.AddUndirected("1", "2") // step 1 from entity 1
	graph8.AddUndirected("1", "7") // step 1 from entity 1
	graph8.AddUndirected("1", "8") // step 1 from entity 1
	graph8.AddUndirected("1", "9") // step 1 from entity 1
	graph8.AddUndirected("4", "5") // step 1 from entity 4
	graph8.AddEntity("6")

	testCases := []struct {
		numberSteps   int
		seedEntities  *set.Set[string]
		expected      *SpiderResults
		errorExpected error
	}{
		{
			numberSteps:   -1, // invalid number of steps
			seedEntities:  set.NewPopulatedSet("1"),
			expected:      nil,
			errorExpected: ErrInvalidNumberSteps,
		},
		{
			numberSteps:   0,
			seedEntities:  set.NewSet[string](), // invalid seed entities
			expected:      nil,
			errorExpected: ErrNoSeedEntities,
		},
		{
			numberSteps:  0,
			seedEntities: set.NewPopulatedSet("1"),
			expected: &SpiderResults{
				NumberSteps:          0,
				Subgraph:             graph2,
				SeedEntities:         set.NewPopulatedSet("1"),
				SeedEntitiesNotFound: set.NewSet[string](),
			},
			errorExpected: nil,
		},
		{
			// Test case with an entity that's not in the unipartite graph
			numberSteps:  0,
			seedEntities: set.NewPopulatedSet("1", "A"),
			expected: &SpiderResults{
				NumberSteps:          0,
				Subgraph:             graph2,
				SeedEntities:         set.NewPopulatedSet("1", "A"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
		{
			// Test case with an entity that's not in the unipartite graph
			numberSteps:  0,
			seedEntities: set.NewPopulatedSet("1", "A", "2"),
			expected: &SpiderResults{
				NumberSteps:          0,
				Subgraph:             graph4,
				SeedEntities:         set.NewPopulatedSet("1", "A", "2"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
		{
			// Test case with 1 step from one entity
			numberSteps:  1,
			seedEntities: set.NewPopulatedSet("1", "A"),
			expected: &SpiderResults{
				NumberSteps:          1,
				Subgraph:             graph5,
				SeedEntities:         set.NewPopulatedSet("1", "A"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
		{
			// Test case with 1 step from two entities
			numberSteps:  1,
			seedEntities: set.NewPopulatedSet("1", "A", "3"),
			expected: &SpiderResults{
				NumberSteps:          1,
				Subgraph:             graph6,
				SeedEntities:         set.NewPopulatedSet("1", "A", "3"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
		{
			// Test case with 2 steps from one entity
			numberSteps:  2,
			seedEntities: set.NewPopulatedSet("1", "A"),
			expected: &SpiderResults{
				NumberSteps:          2,
				Subgraph:             graph7,
				SeedEntities:         set.NewPopulatedSet("1", "A"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
		{
			// Test case with 1 step from two entities in different connected components
			numberSteps:  1,
			seedEntities: set.NewPopulatedSet("1", "A", "4", "6"),
			expected: &SpiderResults{
				NumberSteps:          1,
				Subgraph:             graph8,
				SeedEntities:         set.NewPopulatedSet("1", "A", "4", "6"),
				SeedEntitiesNotFound: set.NewPopulatedSet("A"),
			},
			errorExpected: nil,
		},
	}

	for _, testCase := range testCases {
		graph := makeTestGraph(t)
		s, err := NewSpider(graph)
		assert.NoError(t, err)

		// Execute spidering
		result, err := s.Execute(testCase.numberSteps, testCase.seedEntities)

		// Check the error returned
		assert.Equal(t, testCase.errorExpected, err)

		// Check the results
		if testCase.expected == nil {
			assert.Nil(t, result)
		} else {
			equal, err := testCase.expected.Equal(result)
			assert.NoError(t, err)
			assert.True(t, equal)
		}
	}
}

func TestHasAtLeastOneConnection(t *testing.T) {

	subgraph1 := graphstore.NewInMemoryUnipartiteGraphStore()
	subgraph1.AddEntity("e-1")
	subgraph1.AddEntity("e-2")

	subgraph2 := graphstore.NewInMemoryUnipartiteGraphStore()
	subgraph2.AddDirected("e-1", "e-2")
	subgraph2.AddEntity("e-3")

	testCases := []struct {
		results  *SpiderResults
		expected bool
	}{
		{
			results: &SpiderResults{
				Subgraph: subgraph1,
			},
			expected: false,
		},
		{
			results: &SpiderResults{
				Subgraph: subgraph2,
			},
			expected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := testCase.results.HasAtLeastOneConnection()
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, actual)
	}
}
