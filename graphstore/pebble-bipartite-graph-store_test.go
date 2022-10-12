package graphstore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPebbleKeyForEntity(t *testing.T) {
	id := "e-1"
	key := bipartiteEntityIdToPebbleKey(id)

	// Valid entity key
	entityId, err := pebbleKeyToBipartiteEntityId(key)
	assert.NoError(t, err)
	assert.Equal(t, id, entityId)

	// Invalid entity key
	entityId, err = pebbleKeyToBipartiteEntityId([]byte{})
	assert.Equal(t, "", entityId)
	assert.Error(t, err)

	entityId, err = pebbleKeyToBipartiteEntityId([]byte("x"))
	assert.Equal(t, "", entityId)
	assert.Error(t, err)
}

func TestPebbleValueForEntity(t *testing.T) {
	entity, err := NewEntity("e-1", "Person", map[string]string{
		"Name": "Bob Smith",
		"Age":  "32",
	})
	assert.NoError(t, err)

	pebbleValue, err := bipartiteEntityToPebbleValue(&entity)
	assert.NoError(t, err)

	recoveredEntity, err := pebbleValueToBipartiteEntity(pebbleValue)
	assert.NoError(t, err)

	assert.Equal(t, entity, *recoveredEntity)
}

func TestPebbleKeyForDocument(t *testing.T) {
	id := "d-1"
	key := bipartiteDocumentIdToPebbleKey(id)

	// Valid document key
	documentId, err := pebbleKeyToBipartiteDocumentId(key)
	assert.NoError(t, err)
	assert.Equal(t, id, documentId)

	// Invalid entity key
	documentId, err = pebbleKeyToBipartiteDocumentId([]byte{})
	assert.Equal(t, "", documentId)
	assert.Error(t, err)

	documentId, err = pebbleKeyToBipartiteDocumentId([]byte("x"))
	assert.Equal(t, "", documentId)
	assert.Error(t, err)
}

func TestPebbleValueForDocument(t *testing.T) {
	document, err := NewDocument("d-1", "Source A", map[string]string{
		"Name": "Doc-1",
		"Date": "2022-10-09",
	})
	assert.NoError(t, err)

	// Create the Pebble DB value for a document
	pebbleValue, err := bipartiteDocumentToPebbleValue(&document)
	assert.NoError(t, err)

	// Convert the Pebble DB value back to a document
	recoveredDocument, err := pebbleValueToBipartiteDocument(pebbleValue)
	assert.NoError(t, err)
	assert.Equal(t, document, *recoveredDocument)
}

// newBipartitePebbleStore constructs a new (temporary) bipartite store.
func newBipartitePebbleStore(t *testing.T) *PebbleBipartiteGraphStore {
	folder := createTempPebbleFolder(t)
	store, err := NewPebbleBipartiteGraphStore(folder)
	assert.NoError(t, err)
	return store
}

func cleanUpBipartitePebbleStore(t *testing.T, store *PebbleBipartiteGraphStore) {
	assert.NoError(t, store.Close())
	assert.NoError(t, os.RemoveAll(store.folder))
}

func TestAddBipartiteEntity(t *testing.T) {
	store := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, store)

	// Create an entity to store
	e1, err := NewEntity("e-1", "Person", map[string]string{
		"Name": "Bob Smith",
		"Age":  "32",
	})
	assert.NoError(t, err)

	// Store the entity
	assert.NoError(t, store.AddEntity(e1))

	// Get the entity
	eRecovered, err := store.GetEntity("e-1")
	assert.NoError(t, err)
	assert.Equal(t, e1, *eRecovered)

	// Try to get an entity that doesn't exist
	eRecovered, err = store.GetEntity("e-2")
	assert.Nil(t, eRecovered)
	assert.Equal(t, ErrEntityNotFound, err)

	// Check if the entity is in the store
	found, err := store.HasEntity(&e1)
	assert.NoError(t, err)
	assert.True(t, found)

	e2, err := NewEntity("e-2", "Person", map[string]string{
		"Name": "Sally Jones",
		"Age":  "32",
	})

	found, err = store.HasEntity(&e2)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestAddBipartiteDocument(t *testing.T) {
	store := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, store)

	// Create the document to store
	d1, err := NewDocument("d-1", "Source A", map[string]string{
		"Name": "Document 1",
		"Date": "11/10/2022",
	})
	assert.NoError(t, err)

	// Store the document
	assert.NoError(t, store.AddDocument(d1))

	// Get the document
	dRecovered, err := store.GetDocument("d-1")
	assert.NoError(t, err)
	assert.Equal(t, d1, *dRecovered)

	// Try to get a document that doesn't exist
	dRecovered, err = store.GetDocument("d-2")
	assert.Nil(t, dRecovered)
	assert.Equal(t, ErrDocumentNotFound, err)

	// Check if the store contains the document
	found, err := store.HasDocument(&d1)
	assert.NoError(t, err)
	assert.True(t, found)

	d2, err := NewDocument("d-2", "Source A", map[string]string{
		"Name": "Document 2",
		"Date": "11/10/2022",
	})

	found, err = store.HasDocument(&d2)
	assert.NoError(t, err)
	assert.False(t, found)
}
