package graphstore

type InMemoryBipartiteGraphStore struct {
	entities  map[string]Entity   // Entity ID to Entity mapping
	documents map[string]Document // Document ID to Document mapping
}

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
	store.entities[entity.Id] = entity
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
	store.documents[document.Id] = document
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

	entity, found := store.entities[entityId]

	if found {
		return &entity, nil
	}
	return nil, nil
}

// GetDocument given its ID.
func (store *InMemoryBipartiteGraphStore) GetDocument(documentId string) (*Document, error) {

	// Preconditions
	err := ValidateDocumentId(documentId)
	if err != nil {
		return nil, ErrDocumentIdIsEmpty
	}

	document, found := store.documents[documentId]

	if found {
		return &document, nil
	}
	return nil, ErrDocumentNotFound
}

// AddLink from an entity to a document.
func (store *InMemoryBipartiteGraphStore) AddLink(link Link) error {

	// Preconditions
	err := ValidateEntityId(link.EntityId)
	if err != nil {
		return ErrEntityIdIsEmpty
	}

	err = ValidateDocumentId(link.DocumentId)
	if err != nil {
		return ErrDocumentIdIsEmpty
	}

	// Get the entity from the store
	entity, err := store.GetEntity(link.EntityId)
	if err != nil {
		return err
	}
	if entity == nil {
		return ErrEntityNotFound
	}

	// Get the document from the store
	document, err := store.GetDocument(link.DocumentId)
	if err != nil {
		return err
	}
	if document == nil {
		return ErrDocumentNotFound
	}

	// Make the connections
	entity.AddDocument(link.DocumentId)
	document.AddEntity(link.EntityId)

	return nil
}

// NumberOfEntities in the graph store.
func (store *InMemoryBipartiteGraphStore) NumberOfEntities() (int, error) {
	return len(store.entities), nil
}

// NumberOfDocuments in the graph store.
func (store *InMemoryBipartiteGraphStore) NumberOfDocuments() (int, error) {
	return len(store.documents), nil
}

// Clear the store.
func (store *InMemoryBipartiteGraphStore) Clear() error {

	store.entities = map[string]Entity{}
	store.documents = map[string]Document{}

	return nil
}

// Destroy the bipartite graph store.
func (store *InMemoryBipartiteGraphStore) Destroy() error {
	return store.Clear()
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
