package graphstore

import "fmt"

// BipartiteToUnipartite converter to load a unipartite graph from a bipartite graph.
func BipartiteToUnipartite(bi BipartiteGraphStore, uni UnipartiteGraphStore) error {

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

		// Add the entity/entities to the graph
		if len(entityIds) == 1 {
			uni.AddEntity(entityIds[0])

		} else if len(entityIds) > 1 {
			for _, e1 := range entityIds {
				for _, e2 := range entityIds {
					if e1 == e2 {
						continue
					}

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
