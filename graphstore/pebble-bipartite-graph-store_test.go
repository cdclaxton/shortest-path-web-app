package graphstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestEntityIdToPebbleKey(t *testing.T) {
	entityIds := []string{"e-1", "1", "1234567890", "abcdef"}

	for _, entityId := range entityIds {
		key, err := entityIdToPebbleKey(entityId)
		assert.NoError(t, err)

		recoveredEntityId, err := pebbleKeyToEntityId(key)
		assert.NoError(t, err)
		assert.Equal(t, entityId, recoveredEntityId)
	}
}

func TestEntityToPebbleValue(t *testing.T) {
	pebbleEntities := []PebbleEntity{
		{
			Id:         "e-1",
			EntityType: "",
			Attributes: map[string]string{},
		},
		{
			Id:         "e-1",
			EntityType: "Person",
			Attributes: map[string]string{},
		},
		{
			Id:         "e-1",
			EntityType: "Person",
			Attributes: map[string]string{
				"name": "John Smith",
				"age":  "21",
			},
		},
	}

	for _, pebbleEntity := range pebbleEntities {
		value, err := entityToPebbleValue(&pebbleEntity)
		assert.NoError(t, err)

		recoveredEntity, err := pebbleValueToEntity(value)
		assert.NoError(t, err)
		assert.NotNil(t, recoveredEntity)
		assert.Equal(t, pebbleEntity, *recoveredEntity)
	}
}

func TestValidateDocumentId(t *testing.T) {
	testCases := []struct {
		id            string
		expectedError error
	}{
		{
			id:            "",
			expectedError: ErrEmptyDocumentId,
		},
		{
			id:            separator,
			expectedError: ErrDocumentIdContainsIllegalCharacter,
		},
		{
			id:            "d1",
			expectedError: nil,
		},
	}

	for _, testCase := range testCases {
		err := validateDocumentId(testCase.id)
		assert.ErrorIs(t, err, testCase.expectedError)
	}
}

func TestDocumentIdToPebbleKey(t *testing.T) {
	documentIds := []string{"1", "d1", "1234", "abc"}

	for _, docId := range documentIds {
		key, err := documentIdToPebbleKey(docId)
		assert.NoError(t, err)

		recoveredDocId, err := pebbleKeyToDocumentId(key)
		assert.NoError(t, err)
		assert.Equal(t, docId, recoveredDocId)
	}
}

func TestDocumentToPebbleValue(t *testing.T) {
	documents := []PebbleDocument{
		{
			Id:           "d1",
			DocumentType: "",
			Attributes:   map[string]string{},
		},
		{
			Id:           "1234",
			DocumentType: "Type-A",
			Attributes:   map[string]string{},
		},
		{
			Id:           "d1",
			DocumentType: "",
			Attributes: map[string]string{
				"date":   "2023-06-08",
				"author": "John Smith",
			},
		},
	}

	for _, doc := range documents {
		value, err := documentToPebbleValue(&doc)
		assert.NoError(t, err)

		recoveredDoc, err := pebbleValueToDocument(value)
		assert.NoError(t, err)
		assert.NotNil(t, recoveredDoc)
		assert.Equal(t, doc, *recoveredDoc)
	}
}

func TestEntityDocumentLinkToPebbleKey(t *testing.T) {
	testCases := []struct {
		entityId   string
		documentId string
	}{
		{
			entityId:   "e-1",
			documentId: "d-1",
		},
		{
			entityId:   "1",
			documentId: "1",
		},
		{
			entityId:   "1",
			documentId: "10",
		},
		{
			entityId:   "10",
			documentId: "1",
		},
	}

	for _, testCase := range testCases {
		key, err := entityDocumentLinkToPebbleKey(testCase.entityId, testCase.documentId)
		assert.NoError(t, err)

		ent2, doc2, err := pebbleKeyToEntityDocumentLink(key)
		assert.NoError(t, err)
		assert.Equal(t, testCase.entityId, ent2)
		assert.Equal(t, testCase.documentId, doc2)
	}
}

func TestDocumentEntityLinkToPebbleKey(t *testing.T) {
	testCases := []struct {
		documentId string
		entityId   string
	}{
		{
			documentId: "d-1",
			entityId:   "e-1",
		},
		{
			documentId: "1",
			entityId:   "1",
		},
		{
			documentId: "1",
			entityId:   "10",
		},
		{
			documentId: "10",
			entityId:   "1",
		},
	}

	for _, testCase := range testCases {
		key, err := documentEntityLinkToPebbleKey(testCase.documentId, testCase.entityId)
		assert.NoError(t, err)

		doc2, ent2, err := pebbleKeyToDocumentEntityLink(key)
		assert.NoError(t, err)
		assert.Equal(t, testCase.entityId, ent2)
		assert.Equal(t, testCase.documentId, doc2)
	}
}

// newBipartitePebbleStore constructs a new (temporary) bipartite store.
func newBipartitePebbleStore(t *testing.T) *PebbleBipartiteGraphStore {
	folder := createTempPebbleFolder(t)
	store, err := NewPebbleBipartiteGraphStore(folder)
	assert.NoError(t, err)
	return store
}

func cleanUpBipartitePebbleStore(t *testing.T, store *PebbleBipartiteGraphStore) {
	assert.NoError(t, store.Destroy())
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
	assert.NoError(t, err)

	found, err = store.HasEntity(&e2)
	assert.NoError(t, err)
	assert.False(t, found)
}

func BenchmarkAddEntity(b *testing.B) {

	b.StopTimer()

	for i := 0; i < b.N; i++ {

		// Create the temp folder
		dir, err := ioutil.TempDir("", "pebble")
		if err != nil {
			panic(err)
		}

		// Create a new Pebble-backed bipartite store
		store, err := NewPebbleBipartiteGraphStore(dir)
		if err != nil {
			panic(err)
		}

		b.StartTimer()

		// Add a set number of entities to the store
		for entityIdx := 0; entityIdx < 10000; entityIdx++ {

			entityId := fmt.Sprintf("e-%d", entityIdx)

			// Create an entity to store
			e1, err := NewEntity(entityId, "Person", map[string]string{
				"Name": "Bob Smith",
				"Age":  "32",
			})
			if err != nil {
				panic(err)
			}

			// Store the entity
			err = store.AddEntity(e1)
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()

		store.Destroy()
		os.RemoveAll(dir)
	}
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
	assert.NoError(t, err)

	found, err = store.HasDocument(&d2)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestAddLink(t *testing.T) {
	store := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, store)

	// Create the entity
	e1, err := NewEntity("e-1", "Person", map[string]string{
		"Name": "Bob Smith",
	})
	assert.NoError(t, err)

	// Store the entity
	assert.NoError(t, store.AddEntity(e1))

	// Create the document
	d1, err := NewDocument("d-1", "Source A", map[string]string{
		"Date": "12/10/2022",
	})
	assert.NoError(t, err)

	// Store the document
	assert.NoError(t, store.AddDocument(d1))

	// Create the link
	assert.NoError(t, store.AddLink(NewLink("e-1", "d-1")))

	// Get the entity and check the link is present
	entity1, err := store.GetEntity("e-1")
	assert.NoError(t, err)
	assert.True(t, entity1.LinkedDocumentIds.Equal(set.NewPopulatedSet("d-1")))

	// Get the document and check the link is present
	document1, err := store.GetDocument("d-1")
	assert.NoError(t, err)
	assert.True(t, document1.LinkedEntityIds.Equal(set.NewPopulatedSet("e-1")))
}

func checkExpectedDocumentIds(t *testing.T, store *PebbleBipartiteGraphStore,
	expectedDocIds *set.Set[string]) {

	// Get a document ID iterator for the store
	iter, err := store.NewDocumentIdIterator()
	assert.NoError(t, err)

	// Get all of the document IDs
	docIds, err := AllDocuments(iter)
	assert.NoError(t, err)

	assert.True(t, expectedDocIds.Equal(docIds))

	// Check the number of documents
	nDocuments, err := store.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, expectedDocIds.Len(), nDocuments)
}

func TestDocumentIterator(t *testing.T) {
	store := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, store)

	// No documents in the store
	checkExpectedDocumentIds(t, store, set.NewSet[string]())

	// Add one document
	d1, err := NewDocument("d-1", "Source A", map[string]string{})
	assert.NoError(t, err)
	assert.NoError(t, store.AddDocument(d1))
	checkExpectedDocumentIds(t, store, set.NewPopulatedSet("d-1"))

	// Add another document
	d2, err := NewDocument("d-2", "Source A", map[string]string{})
	assert.NoError(t, err)
	assert.NoError(t, store.AddDocument(d2))
	checkExpectedDocumentIds(t, store, set.NewPopulatedSet("d-1", "d-2"))
}

func checkExpectedEntityIds(t *testing.T, store *PebbleBipartiteGraphStore,
	expectedEntityIds *set.Set[string]) {

	// Get an entity ID iterator for the store
	iter, err := store.NewEntityIdIterator()
	assert.NoError(t, err)

	// Get all of the entity IDs
	entityIds, err := AllEntities(iter)
	assert.NoError(t, err)

	assert.True(t, expectedEntityIds.Equal(entityIds))

	// Check the number of entities
	nDocuments, err := store.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, expectedEntityIds.Len(), nDocuments)
}

func TestEntityIterator(t *testing.T) {
	store := newBipartitePebbleStore(t)
	defer cleanUpBipartitePebbleStore(t, store)

	// No entities in the store
	checkExpectedEntityIds(t, store, set.NewSet[string]())

	// Add one entity
	e1, err := NewEntity("e-1", "Person", map[string]string{})
	assert.NoError(t, err)
	assert.NoError(t, store.AddEntity(e1))

	checkExpectedEntityIds(t, store, set.NewPopulatedSet("e-1"))

	// Add another entity
	e2, err := NewEntity("e-2", "Person", map[string]string{})
	assert.NoError(t, err)
	assert.NoError(t, store.AddEntity(e2))

	checkExpectedEntityIds(t, store, set.NewPopulatedSet("e-1", "e-2"))
}
