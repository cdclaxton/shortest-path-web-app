package graphstore

import (
	"testing"

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

func AddSingleEntity(t *testing.T, store GraphStore) {
	entities := buildEntities(t)

	assert.Equal(t, 0, store.NumberOfEntities())
	assert.Equal(t, 0, store.NumberOfDocuments())

	assert.NoError(t, store.AddEntity(entities[0]))

	assert.Equal(t, 1, store.NumberOfEntities())
	assert.Equal(t, 0, store.NumberOfDocuments())

	// Try to get the entity from the store that should exist
	retrieved := store.GetEntity(entities[0].Id)
	assert.True(t, entities[0].Equal(retrieved))

	// Try to get an entity that shouldn't exist
	assert.Nil(t, store.GetEntity("unknown"))
}

func AddSingleDocument(t *testing.T, store GraphStore) {
	documents := buildDocuments(t)

	assert.Equal(t, 0, store.NumberOfEntities())
	assert.Equal(t, 0, store.NumberOfDocuments())

	assert.NoError(t, store.AddDocument(documents[0]))

	assert.Equal(t, 0, store.NumberOfEntities())
	assert.Equal(t, 1, store.NumberOfDocuments())

	// Try to get the document from the store that should exist
	retrieved := store.GetDocument(documents[0].Id)
	assert.True(t, documents[0].Equal(retrieved))

	// Try to get a document that shouldn't exist
	assert.Nil(t, store.GetDocument("unknown"))
}

func AddLink(t *testing.T, store GraphStore) {
	entities := buildEntities(t)
	documents := buildDocuments(t)

	assert.NoError(t, store.AddEntity(entities[0]))
	assert.NoError(t, store.AddDocument(documents[0]))

	assert.NoError(t, store.AddLink(entities[0].Id, documents[0].Id))

	e0 := store.GetEntity(entities[0].Id)
	assert.NotNil(t, e0)

	d0 := store.GetDocument(documents[0].Id)
	assert.NotNil(t, d0)

	assert.True(t, e0.HasDocument(d0.Id))
	assert.True(t, d0.HasEntity(e0.Id))
}

func AddDuplicateEntity(t *testing.T, store GraphStore) {
	entities := buildEntities(t)

	assert.NoError(t, store.AddEntity(entities[0]))
	assert.NoError(t, store.AddEntity(entities[1]))

	// Try to add the entities again
	assert.Error(t, store.AddEntity(entities[0]))
	assert.Error(t, store.AddEntity(entities[1]))
}

func AddDuplicateDocument(t *testing.T, store GraphStore) {
	documents := buildDocuments(t)

	assert.NoError(t, store.AddDocument(documents[0]))
	assert.NoError(t, store.AddDocument(documents[1]))

	// Try to add the documents again
	assert.Error(t, store.AddDocument(documents[0]))
	assert.Error(t, store.AddDocument(documents[1]))
}

func TestInMemoryGraphStore(t *testing.T) {

	gs := NewInMemoryGraphStore()
	AddSingleEntity(t, gs)

	gs.Clear()
	AddSingleDocument(t, gs)

	gs.Clear()
	AddLink(t, gs)

	gs.Clear()
	AddDuplicateEntity(t, gs)

	gs.Clear()
	AddDuplicateDocument(t, gs)
}
