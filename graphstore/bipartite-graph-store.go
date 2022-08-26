package graphstore

import "github.com/rs/zerolog/log"

// DocumentIdIterator iterates through all document IDs held in the store.
type DocumentIdIterator interface {
	nextDocumentId() string // Get the next document ID
	hasNext() bool          // Does the iterator have another document ID?
}

// EntityIdIterator iterates through all entity IDs held in the store.
type EntityIdIterator interface {
	nextEntityId() string // Get the next entity ID
	hasNext() bool        // Does the iterator have another entity ID?
}

// A BipartiteGraphStore holds entities and documents.
type BipartiteGraphStore interface {
	AddEntity(Entity) error                    // Add an entity to the store
	AddDocument(Document) error                // Add a document to the store
	AddLink(Link) error                        // Add a link from an entity to a document (by ID)
	Clear() error                              // Clear the store
	Equal(BipartiteGraphStore) bool            // Do two stores have the same contents?
	GetEntity(string) *Entity                  // Get an entity by entity ID
	GetDocument(string) *Document              // Get a document by document ID
	HasDocument(*Document) bool                // Does the graph store contain the document?
	HasEntity(*Entity) bool                    // Does the graph store contain the entity?
	NewDocumentIdIterator() DocumentIdIterator // Get a document ID iterator
	NewEntityIdIterator() EntityIdIterator     // Get an entity ID iterator
	NumberOfEntities() int                     // Number of entities in the store
	NumberOfDocuments() int                    // Number of documents in the store
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
func equalEntities(ref BipartiteGraphStore, test BipartiteGraphStore) bool {

	refEntityIdIterator := ref.NewEntityIdIterator()

	for refEntityIdIterator.hasNext() {
		entityId := refEntityIdIterator.nextEntityId()
		log.Debug().Str("Component", "GraphStore").Msgf("Checking entity %v", entityId)

		// Get the entity from the reference store
		refEntity := ref.GetEntity(entityId)

		// Does the test store contain the entity with the required ID?
		testEntity := test.GetEntity(entityId)
		if testEntity == nil {
			log.Debug().Str("Component", "GraphStore").Msgf("Failed to find entity %v", entityId)
			return false
		}

		// Check whether the entities are equal
		if !refEntity.Equal(testEntity) {
			log.Debug().Str("Component", "GraphStore").Msgf("Entities with ID %v are not equal", entityId)
			log.Debug().Str("Component", "GraphStore").Msgf("Reference entity: %v", refEntity.String())
			log.Debug().Str("Component", "GraphStore").Msgf("Test entity: %v", testEntity.String())

			return false
		}

		log.Debug().Str("Component", "GraphStore").Msgf("Entities with ID %v are equal", entityId)
	}

	return true
}

// equalDocuments exist in two bipartite graph stores?
func equalDocuments(ref BipartiteGraphStore, test BipartiteGraphStore) bool {

	refDocumentIterator := ref.NewDocumentIdIterator()

	for refDocumentIterator.hasNext() {
		documentId := refDocumentIterator.nextDocumentId()

		// Get the document from the reference store
		refDocument := ref.GetDocument(documentId)

		// Does the test store contain the entity with the required ID?
		testDocument := test.GetDocument(documentId)
		if testDocument == nil {
			return false
		}

		// Check whether the documents are equal
		if !refDocument.Equal(testDocument) {
			return false
		}
	}

	return true
}

// bipartiteGraphStoresEqual returns true if two stores have the same contents.
// This function is used for testing purposes.
func bipartiteGraphStoresEqual(s1 BipartiteGraphStore, s2 BipartiteGraphStore) bool {
	return equalEntities(s1, s2) && equalEntities(s2, s1) &&
		equalDocuments(s1, s2) && equalDocuments(s2, s1)
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
