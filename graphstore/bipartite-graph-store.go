package graphstore

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/rs/zerolog/log"
)

// DocumentIdIterator iterates through all document IDs held in the store.
type DocumentIdIterator interface {
	nextDocumentId() (string, error) // Get the next document ID
	hasNext() bool                   // Does the iterator have another document ID?
}

// EntityIdIterator iterates through all entity IDs held in the store.
type EntityIdIterator interface {
	nextEntityId() (string, error) // Get the next entity ID
	hasNext() bool                 // Does the iterator have another entity ID?
}

// A BipartiteGraphStore holds entities and documents.
type BipartiteGraphStore interface {
	AddEntity(Entity) error                             // Add an entity to the store
	AddDocument(Document) error                         // Add a document to the store
	AddLink(Link) error                                 // Add a link from an entity to a document (by ID)
	Clear() error                                       // Clear the store
	Equal(BipartiteGraphStore) (bool, error)            // Do two stores have the same contents?
	GetEntity(string) *Entity                           // Get an entity by entity ID
	GetDocument(string) *Document                       // Get a document by document ID
	HasDocument(*Document) bool                         // Does the graph store contain the document?
	HasEntity(*Entity) bool                             // Does the graph store contain the entity?
	NewDocumentIdIterator() (DocumentIdIterator, error) // Get a document ID iterator
	NewEntityIdIterator() (EntityIdIterator, error)     // Get an entity ID iterator
	NumberOfEntities() (int, error)                     // Number of entities in the store
	NumberOfDocuments() (int, error)                    // Number of documents in the store
}

// BulkLoadBipartiteGraphStore given entities, documents and links.
func BulkLoadBipartiteGraphStore(graph BipartiteGraphStore, entities []Entity,
	documents []Document, links []Link) error {

	// Load the entities
	for _, entity := range entities {
		if err := graph.AddEntity(entity); err != nil {
			return err
		}
	}

	// Load the documents
	for _, document := range documents {
		if err := graph.AddDocument(document); err != nil {
			return err
		}
	}

	// Load the links
	for _, link := range links {
		if err := graph.AddLink(link); err != nil {
			return err
		}
	}

	return nil
}

// equalEntities in two bipartite graph stores?
func equalEntities(ref BipartiteGraphStore, test BipartiteGraphStore) (bool, error) {

	refEntityIdIterator, err := ref.NewEntityIdIterator()
	if err != nil {
		return false, err
	}

	for refEntityIdIterator.hasNext() {
		entityId, err := refEntityIdIterator.nextEntityId()
		if err != nil {
			return false, err
		}
		log.Debug().Str("Component", "GraphStore").Msgf("Checking entity %v", entityId)

		// Get the entity from the reference store
		refEntity := ref.GetEntity(entityId)

		// Does the test store contain the entity with the required ID?
		testEntity := test.GetEntity(entityId)
		if testEntity == nil {
			log.Debug().Str("Component", "GraphStore").Msgf("Failed to find entity %v", entityId)
			return false, nil
		}

		// Check whether the entities are equal
		if !refEntity.Equal(testEntity) {
			log.Debug().Str("Component", "GraphStore").Msgf("Entities with ID %v are not equal", entityId)
			log.Debug().Str("Component", "GraphStore").Msgf("Reference entity: %v", refEntity.String())
			log.Debug().Str("Component", "GraphStore").Msgf("Test entity: %v", testEntity.String())

			return false, nil
		}

		log.Debug().Str("Component", "GraphStore").Msgf("Entities with ID %v are equal", entityId)
	}

	return true, nil
}

// equalDocuments exist in two bipartite graph stores?
func equalDocuments(ref BipartiteGraphStore, test BipartiteGraphStore) (bool, error) {

	refDocumentIterator, err := ref.NewDocumentIdIterator()
	if err != nil {
		return false, err
	}

	for refDocumentIterator.hasNext() {
		documentId, err := refDocumentIterator.nextDocumentId()
		if err != nil {
			return false, err
		}

		// Get the document from the reference store
		refDocument := ref.GetDocument(documentId)

		// Does the test store contain the entity with the required ID?
		testDocument := test.GetDocument(documentId)
		if testDocument == nil {
			return false, nil
		}

		// Check whether the documents are equal
		if !refDocument.Equal(testDocument) {
			return false, nil
		}
	}

	return true, nil
}

// bipartiteGraphStoresEqual returns true if two stores have the same contents.
// This function is used for testing purposes.
func bipartiteGraphStoresEqual(s1 BipartiteGraphStore, s2 BipartiteGraphStore) (bool, error) {

	// Check the entities
	t1, err := equalEntities(s1, s2)
	if err != nil {
		return false, err
	}

	t2, err := equalEntities(s2, s1)
	if err != nil {
		return false, err
	}

	// Check the documents
	t3, err := equalDocuments(s1, s2)
	if err != nil {
		return false, err
	}

	t4, err := equalDocuments(s2, s1)
	if err != nil {
		return false, err
	}

	return t1 && t2 && t3 && t4, nil
}

// attributesEqual for two maps?
func attributesEqual(m1 map[string]string, m2 map[string]string) bool {

	if len(m1) != len(m2) {
		return false
	}

	for key, value := range m1 {
		value2, found := m2[key]

		// Key is missing
		if !found {
			return false
		}

		// Different values
		if value != value2 {
			return false
		}
	}

	return true
}

// AllDocuments available in the iterator.
func AllDocuments(iter DocumentIdIterator) (*set.Set[string], error) {

	// Preconditions
	if iter == nil {
		return nil, fmt.Errorf("Document iterator is nil")
	}

	ids := set.NewSet[string]()

	for iter.hasNext() {
		id, err := iter.nextDocumentId()
		if err != nil {
			return nil, err
		}
		ids.Add(id)
	}

	return ids, nil
}

// AllEntities available in the iterator.
func AllEntities(iter EntityIdIterator) (*set.Set[string], error) {

	// Preconditions
	if iter == nil {
		return nil, fmt.Errorf("Entity iterator is nil")
	}

	ids := set.NewSet[string]()

	for iter.hasNext() {
		id, err := iter.nextEntityId()
		if err != nil {
			return nil, err
		}
		ids.Add(id)
	}

	return ids, nil
}
