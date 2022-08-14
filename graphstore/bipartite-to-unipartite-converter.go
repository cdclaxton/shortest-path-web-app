package graphstore

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

// BipartiteToUnipartite converter to load a unipartite graph from a bipartite graph.
//
// The set of skipEntities are those entities that won't be transferred to the
// unipartite graph.
func BipartiteToUnipartite(bi BipartiteGraphStore, uni UnipartiteGraphStore,
	skipEntities *set.Set[string]) error {

	// Iterator to retrieve documents from the bipartite graph store
	it := bi.NewDocumentIdIterator()

	for it.hasNext() {

		// Get the next document ID
		docId := it.nextDocumentId()

		// Get the document given its ID
		doc := bi.GetDocument(docId)
		if doc == nil {
			return fmt.Errorf("Document doesn't exist with ID: %v", docId)
		}

		// Entity IDs related to the document
		entityIds := doc.LinkedEntityIds.ToSlice()

		// If there is just a single entity, add it to the graph
		if len(entityIds) == 1 {
			uni.AddEntity(entityIds[0])
			continue
		}

		// Add the entities to the graph
		for _, e1 := range entityIds {

			if skipEntities.Has(e1) {
				continue
			}

			for _, e2 := range entityIds {

				if skipEntities.Has(e2) {
					continue
				}

				if e1 != e2 {
					// Add the link
					err := uni.AddUndirected(e1, e2)
					if err != nil {
						return err
					}
				}

			}
		}

	}

	return nil
}
