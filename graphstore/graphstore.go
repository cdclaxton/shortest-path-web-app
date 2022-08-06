package graphstore

type GraphStore interface {
	AddEntity(Entity) error       // Add an entity to the store
	AddDocument(Document) error   // Add a document to the store
	AddLink(Link) error           // Add a link from an entity to a document (by ID)
	Clear() error                 // Clear the store
	GetEntity(string) *Entity     // Get an entity by entity ID
	GetDocument(string) *Document // Get a document by document ID
	HasDocument(*Document) bool   // Does the graph store contain the document?
	HasEntity(*Entity) bool       // Does the graph store contain the entity?
	NumberOfEntities() int        // Number of entities in the store
	NumberOfDocuments() int       // Number of documents in the store
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
