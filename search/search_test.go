package search

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {

	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	testCases := []struct {
		entityIds       []string
		expectedResults map[string]EntitySearchResult
	}{
		{
			// One entity (in both graph stores)
			entityIds: []string{"e-1"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
			},
		},
		{
			// One entity (in neither graph store)
			entityIds: []string{"e-5"},
			expectedResults: map[string]EntitySearchResult{
				"e-5": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
		{
			// Two entities
			entityIds: []string{"e-1", "e-2"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-2": {
					InUnipartite: true,
					InBipartite:  true,
				},
			},
		},
		{
			// Three entities
			entityIds: []string{"e-1", "e-2", "e-5"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-2": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-5": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		// Run the search tests
		for _, testCase := range testCases {
			actual, err := engine.Search(testCase.entityIds)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedResults, actual)
		}

		// Destroy the graph databases
		graphBuilder.Destroy()
	}
}
