package graphstore

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

// buildEntities (that are unconnected to documents) for use in tests.
func buildEntities(t *testing.T) []Entity {
	e1, err := NewEntity("e-1", "person", map[string]string{
		"forename": "Bob", "surname": "Smith"})
	assert.NoError(t, err)

	e2, err := NewEntity("e-2", "person", map[string]string{
		"forename": "Sarah", "surname": "Thorp"})
	assert.NoError(t, err)

	return []Entity{e1, e2}
}

func buildDocuments(t *testing.T) []Document {
	d1, err := NewDocument("doc-1", "info", map[string]string{"date": "2022-07-22"})
	assert.NoError(t, err)

	d2, err := NewDocument("doc-2", "info", map[string]string{"date": "2022-07-10"})
	assert.NoError(t, err)

	return []Document{d1, d2}
}

func AddSingleEntity(t *testing.T, store BipartiteGraphStore) {
	entities := buildEntities(t)

	// No entities or documents
	nEntities, err := store.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, 0, nEntities)

	nDocuments, err := store.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, 0, nDocuments)

	// Add an entity
	assert.NoError(t, store.AddEntity(entities[0]))

	nEntities, err = store.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, 1, nEntities)

	nDocuments, err = store.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, 0, nDocuments)

	// Try to get the entity from the store that should exist
	retrieved, err := store.GetEntity(entities[0].Id)
	assert.NoError(t, err)
	assert.True(t, entities[0].Equal(retrieved))

	// Try to get an entity that shouldn't exist
	retrieved, err = store.GetEntity("unknown")
	assert.NoError(t, err)
	assert.Nil(t, retrieved)
}

func AddSingleDocument(t *testing.T, store BipartiteGraphStore) {
	documents := buildDocuments(t)

	nEntities, err := store.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, 0, nEntities)

	nDocuments, err := store.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, 0, nDocuments)

	assert.NoError(t, store.AddDocument(documents[0]))

	nEntities, err = store.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, 0, nEntities)

	nDocuments, err = store.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, 1, nDocuments)

	// Try to get the document from the store that should exist
	retrieved, err := store.GetDocument(documents[0].Id)
	assert.NoError(t, err)
	assert.True(t, documents[0].Equal(retrieved))

	// Try to get a document that shouldn't exist
	retrieved, err = store.GetDocument("unknown")
	assert.Equal(t, ErrDocumentNotFound, err)
	assert.Nil(t, retrieved)
}

func AddLink(t *testing.T, store BipartiteGraphStore) {
	entities := buildEntities(t)
	documents := buildDocuments(t)

	assert.NoError(t, store.AddEntity(entities[0]))
	assert.NoError(t, store.AddDocument(documents[0]))

	l := NewLink(entities[0].Id, documents[0].Id)

	assert.NoError(t, store.AddLink(l))

	e0, err := store.GetEntity(entities[0].Id)
	assert.NoError(t, err)
	assert.NotNil(t, e0)

	d0, err := store.GetDocument(documents[0].Id)
	assert.NoError(t, err)
	assert.NotNil(t, d0)

	assert.True(t, e0.HasDocument(d0.Id))
	assert.True(t, d0.HasEntity(e0.Id))
}

func AddDuplicateEntity(t *testing.T, store BipartiteGraphStore) {
	entities := buildEntities(t)

	assert.NoError(t, store.AddEntity(entities[0]))
	assert.NoError(t, store.AddEntity(entities[1]))

	// Try to add the entities again (they will be overwritten)
	assert.NoError(t, store.AddEntity(entities[0]))
	assert.NoError(t, store.AddEntity(entities[1]))
}

func AddDuplicateDocument(t *testing.T, store BipartiteGraphStore) {
	documents := buildDocuments(t)

	assert.NoError(t, store.AddDocument(documents[0]))
	assert.NoError(t, store.AddDocument(documents[1]))

	// Try to add the documents again (they will be overwritten)
	assert.NoError(t, store.AddDocument(documents[0]))
	assert.NoError(t, store.AddDocument(documents[1]))
}

func DocumentIterator(t *testing.T, store BipartiteGraphStore) {
	documents := buildDocuments(t)

	assert.NoError(t, store.AddDocument(documents[0]))
	assert.NoError(t, store.AddDocument(documents[1]))

	// Get a document ID iterator
	it, err := store.NewDocumentIdIterator()
	assert.NoError(t, err)

	// Expected document IDs
	expectedIds := set.NewPopulatedSet("doc-1", "doc-2")

	// Build a set of the document IDs returned by the iterator
	actualIds := set.NewSet[string]()
	for it.hasNext() {
		docId, err := it.nextDocumentId()
		assert.NoError(t, err)
		actualIds.Add(docId)
	}

	// Check the document IDs are expected
	assert.True(t, expectedIds.Equal(actualIds))
}

func TestInMemoryGraphStore(t *testing.T) {

	// Make the in-memory graph store
	inMemoryGraphStore := NewInMemoryBipartiteGraphStore()

	// Make the Pebble graph store
	pebbleGraphStore := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, pebbleGraphStore)

	graphStores := []BipartiteGraphStore{
		inMemoryGraphStore,
		pebbleGraphStore,
	}

	for _, gs := range graphStores {

		gs = NewInMemoryBipartiteGraphStore()
		AddSingleEntity(t, gs)

		gs.Clear()
		AddSingleDocument(t, gs)

		gs.Clear()
		AddLink(t, gs)

		gs.Clear()
		AddDuplicateEntity(t, gs)

		gs.Clear()
		AddDuplicateDocument(t, gs)

		gs.Clear()
		DocumentIterator(t, gs)
	}

}
