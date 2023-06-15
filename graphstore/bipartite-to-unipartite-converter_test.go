package graphstore

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

// TestBipartiteToUnipartite constructs a bipartite graph and then converts that
// to a unipartite graph.
//
// The big test case is:
//
//   [e-1] --- [doc-1] --- [e-2] --- [doc-3] --- [e-3]
//     |                     |                     |
//     |------ [doc-2] ------|                     |
//                |                             [doc-5]
//              [e-4]                              |
//                |                                |
//             [doc-4] --- [e-5] --- [doc-6] --- [e-6]
//
// The test case with entities that are skipped uses the same graph structure,
// but entity e-2 is ignored.
func TestBipartiteToUnipartite(t *testing.T) {
	testCases := []struct {
		documents           []Document
		skipEntities        *set.Set[string]
		expectedConnections []connection
	}{
		// One document, one entity
		{
			documents: []Document{
				{
					Id:              "doc-1",
					LinkedEntityIds: set.NewPopulatedSet("e-1"),
				},
			},
			skipEntities: set.NewSet[string](),
			expectedConnections: []connection{
				{
					source:       "e-1",
					destinations: []string{},
				},
			},
		},
		// One document, two entities
		{
			documents: []Document{
				{
					Id: "doc-1",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-1", "e-2"),
				},
			},
			skipEntities: set.NewSet[string](),
			expectedConnections: []connection{
				{
					source:       "e-1",
					destinations: []string{"e-2"},
				},
				{
					source:       "e-2",
					destinations: []string{"e-1"},
				},
			},
		},
		// Big test case
		{
			documents: []Document{
				{
					Id: "doc-1",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-1", "e-2"),
				},
				{
					Id: "doc-2",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-1", "e-2", "e-4"),
				},
				{
					Id: "doc-3",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-2", "e-3"),
				},
				{
					Id: "doc-4",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-4", "e-5"),
				},
				{
					Id: "doc-5",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-3", "e-6"),
				},
				{
					Id: "doc-6",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-5", "e-6"),
				},
			},
			skipEntities: set.NewSet[string](),
			expectedConnections: []connection{
				{
					source:       "e-1",
					destinations: []string{"e-2", "e-4"},
				},
				{
					source:       "e-2",
					destinations: []string{"e-1", "e-3", "e-4"},
				},
				{
					source:       "e-3",
					destinations: []string{"e-2", "e-6"},
				},
				{
					source:       "e-4",
					destinations: []string{"e-1", "e-2", "e-5"},
				},
				{
					source:       "e-5",
					destinations: []string{"e-4", "e-6"},
				},
				{
					source:       "e-6",
					destinations: []string{"e-3", "e-5"},
				},
			},
		},
		// Big test case with skipped entities
		{
			documents: []Document{
				{
					Id: "doc-1",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-1", "e-2"),
				},
				{
					Id: "doc-2",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-1", "e-2", "e-4"),
				},
				{
					Id: "doc-3",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-2", "e-3"),
				},
				{
					Id: "doc-4",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-4", "e-5"),
				},
				{
					Id: "doc-5",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-3", "e-6"),
				},
				{
					Id: "doc-6",
					LinkedEntityIds: set.NewPopulatedSet(
						"e-5", "e-6"),
				},
			},
			skipEntities: set.NewPopulatedSet("e-2"),
			expectedConnections: []connection{
				{
					source:       "e-1",
					destinations: []string{"e-4"},
				},
				{
					source:       "e-3",
					destinations: []string{"e-6"},
				},
				{
					source:       "e-4",
					destinations: []string{"e-1", "e-5"},
				},
				{
					source:       "e-5",
					destinations: []string{"e-4", "e-6"},
				},
				{
					source:       "e-6",
					destinations: []string{"e-3", "e-5"},
				},
			},
		},
	}

	for _, testCase := range testCases {
		bi := NewInMemoryBipartiteGraphStore()
		uni := NewInMemoryUnipartiteGraphStore()

		// Add the documents to the bipartite graph
		for _, doc := range testCase.documents {
			assert.NoError(t, bi.AddDocument(doc))
		}

		numWorkers := 2
		jobChannelSize := 2

		// Convert bipartite graph to unipartite graph
		assert.NoError(t, BipartiteToUnipartite(bi, uni, testCase.skipEntities,
			numWorkers, jobChannelSize))

		// Check the unipartite graph
		checkConnections(t, uni, testCase.expectedConnections)
	}
}
