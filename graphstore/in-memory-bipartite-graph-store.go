package graphstore

import "sync"

// InMemoryBipartiteGraphStore holds a bipartite graph of entities and documents in memory.
type InMemoryBipartiteGraphStore struct {
	muEntities sync.RWMutex      // Mutex for the entities
	entities   map[string]Entity // Entity ID to Entity mapping

	muDocuments sync.RWMutex        // Mutex for the documents
	documents   map[string]Document // Document ID to Document mapping
}

// NewInMemoryBipartiteGraphStore creates a new in-memory bipartite graph store.
func NewInMemoryBipartiteGraphStore() *InMemoryBipartiteGraphStore {
	return &InMemoryBipartiteGraphStore{
		entities:  map[string]Entity{},
		documents: map[string]Document{},
	}
}

// AddEntity to the in-memory graph store (replaces the existing entity if the ID already exists).
func (store *InMemoryBipartiteGraphStore) AddEntity(entity Entity) error {

	// Preconditions
	err := ValidateEntityId(entity.Id)
	if err != nil {
		return ErrEntityIdIsEmpty
	}

	// Store the entity against its entity ID
	store.muEntities.Lock()
	store.entities[entity.Id] = entity
	store.muEntities.Unlock()

	return nil
}

// AddDocument to the in-memory graph store (replaces the existing document if the ID already exists).
func (store *InMemoryBipartiteGraphStore) AddDocument(document Document) error {

	// Preconditions
	err := ValidateDocumentId(document.Id)
	if err != nil {
		return ErrDocumentIdIsEmpty
	}

	// Store the document against its ID
	store.muDocuments.Lock()
	store.documents[document.Id] = document
	store.muDocuments.Unlock()

	return nil
}

// Equal returns true if two stores have the same contents.
func (store *InMemoryBipartiteGraphStore) Equal(other BipartiteGraphStore) (bool, error) {
	return bipartiteGraphStoresEqual(store, other)
}

// GetEntity given its ID.
func (store *InMemoryBipartiteGraphStore) GetEntity(entityId string) (*Entity, error) {

	// Preconditions
	err := ValidateEntityId(entityId)
	if err != nil {
		return nil, ErrEntityIdIsEmpty
	}

	store.muEntities.RLock()
	entity, found := store.entities[entityId]
	store.muEntities.RUnlock()

	if found {
		return &entity, nil
	}
	return nil, ErrEntityNotFound
}

// GetDocument given its ID.
func (store *InMemoryBipartiteGraphStore) GetDocument(documentId string) (*Document, error) {

	// Preconditions
	err := ValidateDocumentId(documentId)
	if err != nil {
		return nil, ErrDocumentIdIsEmpty
	}

	store.muDocuments.RLock()
	document, found := store.documents[documentId]
	store.muDocuments.RUnlock()

	if found {
		return &document, nil
	}
	return nil, ErrDocumentNotFound
}

// AddLink from an entity to a document.
func (store *InMemoryBipartiteGraphStore) AddLink(link Link) error {

	// Ensure the entity ID and document ID are valid
	err := ValidateEntityId(link.EntityId)
	if err != nil {
		return ErrEntityIdIsEmpty
	}

	err = ValidateDocumentId(link.DocumentId)
	if err != nil {
		return ErrDocumentIdIsEmpty
	}

	// Get locks
	store.muEntities.Lock()
	store.muDocuments.Lock()
	defer store.muDocuments.Unlock()
	defer store.muEntities.Unlock()

	// Try to get the entity from the store
	entity, found := store.entities[link.EntityId]
	if !found {
		return ErrEntityNotFound
	}

	// Try to get the document from the store
	document, found := store.documents[link.DocumentId]
	if !found {
		return ErrDocumentNotFound
	}

	// Make the connections
	entity.AddDocument(link.DocumentId)
	document.AddEntity(link.EntityId)

	return nil
}

// NumberOfEntities in the graph store.
func (store *InMemoryBipartiteGraphStore) NumberOfEntities() (int, error) {

	store.muEntities.RLock()
	n := len(store.entities)
	store.muEntities.RUnlock()

	return n, nil
}

// NumberOfDocuments in the graph store.
func (store *InMemoryBipartiteGraphStore) NumberOfDocuments() (int, error) {

	store.muDocuments.RLock()
	n := len(store.documents)
	store.muDocuments.RUnlock()

	return n, nil
}

// Clear the store.
func (store *InMemoryBipartiteGraphStore) Clear() error {

	store.muEntities.Lock()
	store.muDocuments.Lock()

	store.entities = map[string]Entity{}
	store.documents = map[string]Document{}

	store.muDocuments.Unlock()
	store.muEntities.Unlock()

	return nil
}

func (graph *InMemoryBipartiteGraphStore) Close() error {
	return nil
}

// Destroy the bipartite graph store.
func (store *InMemoryBipartiteGraphStore) Destroy() error {
	return store.Clear()
}

func (store *InMemoryBipartiteGraphStore) Finalise() error {
	return nil
}

// HasDocument returns true if the graph store contains the document.
func (store *InMemoryBipartiteGraphStore) HasDocument(document *Document) (bool, error) {

	// Try to retrieve the document from the graph store
	retrieved, err := store.GetDocument(document.Id)
	if err != nil {
		return false, err
	}
	if retrieved == nil {
		return false, nil
	}

	// Check the document matches
	return document.Equal(retrieved), nil
}

// Does the graph store contain the entity?
func (store *InMemoryBipartiteGraphStore) HasEntity(entity *Entity) (bool, error) {

	// Try to retrieve the entity from the graph store
	retrieved, err := store.GetEntity(entity.Id)
	if err != nil {
		return false, err
	}
	if retrieved == nil {
		return false, nil
	}

	// Check the entity matches
	return entity.Equal(retrieved), nil
}

func (store *InMemoryBipartiteGraphStore) HasEntityWithId(entityId string) (bool, error) {

	// Try to retrieve the entity from the graph store
	retrieved, err := store.GetEntity(entityId)
	if err == ErrEntityNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return retrieved != nil, nil
}

// An InMemoryDocumentIterator walks through all of the IDs of the documents held
// within the bipartite graph store. Note that the order of the document IDs is not
// guaranteed to be same on different runs.
type InMemoryDocumentIterator struct {
	documentIds  []string
	currentIndex int
}

func (it *InMemoryDocumentIterator) nextDocumentId() (string, error) {
	currentDocumentId := it.documentIds[it.currentIndex]
	it.currentIndex += 1
	return currentDocumentId, nil
}

func (it *InMemoryDocumentIterator) hasNext() bool {
	return it.currentIndex < len(it.documentIds)
}

func (store *InMemoryBipartiteGraphStore) NewDocumentIdIterator() (DocumentIdIterator, error) {

	// Create a slice of document IDs
	documentIds := []string{}
	for docId := range store.documents {
		documentIds = append(documentIds, docId)
	}

	// Return a new iterator
	return &InMemoryDocumentIterator{
		documentIds:  documentIds,
		currentIndex: 0,
	}, nil
}

type InMemoryEntityIterator struct {
	entityIds    []string
	currentIndex int
}

func (it *InMemoryEntityIterator) nextEntityId() (string, error) {
	currentEntityId := it.entityIds[it.currentIndex]
	it.currentIndex += 1
	return currentEntityId, nil
}

func (it *InMemoryEntityIterator) hasNext() bool {
	return it.currentIndex < len(it.entityIds)
}

func (store *InMemoryBipartiteGraphStore) NewEntityIdIterator() (EntityIdIterator, error) {

	// Create a slice of entity IDs
	entityIds := []string{}
	for entityId := range store.entities {
		entityIds = append(entityIds, entityId)
	}

	// Return a new iterator
	return &InMemoryEntityIterator{
		entityIds:    entityIds,
		currentIndex: 0,
	}, nil
}
