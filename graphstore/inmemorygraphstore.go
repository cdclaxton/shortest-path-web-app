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
func (store *InMemoryGraphStore) AddLink(link Link) error {

	// Get the entity from the store
	entity := store.GetEntity(link.EntityId)
	if entity == nil {
		return fmt.Errorf("Entity with ID %v could not be found", link.EntityId)
	}

	// Get the document from the store
	document := store.GetDocument(link.DocumentId)
	if document == nil {
		return fmt.Errorf("Document with ID %v could not be found", link.DocumentId)
	}

	// Make the connections
	entity.AddDocument(link.DocumentId)
	document.AddEntity(link.EntityId)

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

// HasDocument returns true if the graph store contains the document.
func (store *InMemoryGraphStore) HasDocument(document *Document) bool {

	// Try to retrieve the document from the graph store
	retrieved := store.GetDocument(document.Id)
	if retrieved == nil {
		return false
	}

	// Check the document matches
	return document.Equal(retrieved)
}

// Does the graph store contain the entity?
func (store *InMemoryGraphStore) HasEntity(entity *Entity) bool {

	// Try to retrieve the entity from the graph store
	retrieved := store.GetEntity(entity.Id)
	if retrieved == nil {
		return false
	}

	// Check the entity matches
	return entity.Equal(retrieved)
}
