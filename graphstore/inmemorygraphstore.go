package graphstore

import "fmt"

type InMemoryGraphStore struct {
	entities  map[string]Entity   // Entity ID to Entity mapping
	documents map[string]Document // Document ID to Document mapping
}

func NewInMemoryGraphStore() *InMemoryGraphStore {
	return &InMemoryGraphStore{
		entities:  map[string]Entity{},
		documents: map[string]Document{},
	}
}

// AddEntity to the in-memory graph store.
func (store *InMemoryGraphStore) AddEntity(entity Entity) error {

	// Check whether the entity already exists in the store
	if _, found := store.entities[entity.Id]; found {
		return fmt.Errorf("Entity %v already exists", entity.Id)
	}

	// Store the entity against its entity ID
	store.entities[entity.Id] = entity
	return nil
}

// AddDocument to the in-memory graph store.
func (store *InMemoryGraphStore) AddDocument(document Document) error {

	// Check whether the document already exists in the store
	if _, found := store.documents[document.Id]; found {
		return fmt.Errorf("Document %v already exists", document.Id)
	}

	// Store the document against its ID
	store.documents[document.Id] = document
	return nil
}

// GetEntity given its ID.
func (store *InMemoryGraphStore) GetEntity(entityId string) *Entity {

	entity, found := store.entities[entityId]

	if found {
		return &entity
	}
	return nil
}

// GetDocument given its ID.
func (store *InMemoryGraphStore) GetDocument(documentId string) *Document {

	document, found := store.documents[documentId]

	if found {
		return &document
	}
	return nil
}

// AddLink from an entity to a document.
func (store *InMemoryGraphStore) AddLink(entityId string, documentId string) error {

	// Get the entity from the store
	entity := store.GetEntity(entityId)
	if entity == nil {
		return fmt.Errorf("Entity with ID %v could not be found", entityId)
	}

	// Get the document from the store
	document := store.GetDocument(documentId)
	if document == nil {
		return fmt.Errorf("Document with ID %v could not be found", documentId)
	}

	// Make the connections
	entity.AddDocument(documentId)
	document.AddEntity(entityId)

	return nil
}

// NumberOfEntities in the graph store.
func (store *InMemoryGraphStore) NumberOfEntities() int {
	return len(store.entities)
}

// NumberOfDocuments in the graph store.
func (store *InMemoryGraphStore) NumberOfDocuments() int {
	return len(store.documents)
}

// Clear the store
func (store *InMemoryGraphStore) Clear() error {

	store.entities = map[string]Entity{}
	store.documents = map[string]Document{}

	return nil
}
