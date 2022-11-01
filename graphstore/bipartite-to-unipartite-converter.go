package graphstore

import (
	"errors"
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// BipartiteToUnipartite converter to load a unipartite graph from a bipartite graph.
//
// The set of skipEntities are those entities that won't be transferred to the
// unipartite graph.
func BipartiteToUnipartite(bi BipartiteGraphStore, uni UnipartiteGraphStore,
	skipEntities *set.Set[string]) error {

	// Preconditions
	if bi == nil {
		return errors.New("bipartite store is nil")
	}

	if uni == nil {
		return errors.New("unipartite store is nil")
	}

	if skipEntities == nil {
		return errors.New("entities to skip is nil")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting bipartite to unipartite conversion")

	// Iterator to retrieve documents from the bipartite graph store
	it, err := bi.NewDocumentIdIterator()
	if err != nil {
		return err
	}

	for it.hasNext() {

		// Get the next document ID from the iterator
		docId, err := it.nextDocumentId()
		if err != nil {
			return err
		}

		// Get the document given its ID
		doc, err := bi.GetDocument(docId)
		if err != nil {
			return err
		}
		if doc == nil {
			return fmt.Errorf("document doesn't exist with ID: %v", docId)
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

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Finished bipartite to unipartite conversion")

	return nil
}
