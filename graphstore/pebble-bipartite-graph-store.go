// This implementation of the bipartite graph store uses Pebble DB as the
// backend. The bipartite graph is composed of entities and documents.
//
// Entities are stored as:
//
//   e#<Entity ID> --> serialised version of an Entity (using GOB)
//
// Similarly, documents are stored as:
//
//   d#<Document ID> --> serialised version of a Document (using GOB)

package graphstore

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type PebbleBipartiteGraphStore struct {
	folder string
	db     *pebble.DB
}

func NewPebbleBipartiteGraphStore(folder string) (*PebbleBipartiteGraphStore, error) {
	db, err := pebble.Open(folder, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	store := PebbleBipartiteGraphStore{
		folder: folder,
		db:     db,
	}

	return &store, nil
}

func (p *PebbleBipartiteGraphStore) Close() error {
	return p.db.Close()
}

// Prefixes for the keys used by Pebble
const (
	entityKeyPrefix   = "e"
	documentKeyPrefix = "d"
)

// bipartiteEntityIdToPebbleKey converts an entity ID to a Pebble key.
func bipartiteEntityIdToPebbleKey(id string) []byte {
	return []byte(entityKeyPrefix + id)
}

// pebbleKeyToBipartiteEntityId converts a Pebble key to an entity ID.
func pebbleKeyToBipartiteEntityId(value []byte) (string, error) {

	// Preconditions
	if value == nil {
		return "", fmt.Errorf("Entity key is nil")
	}

	keyString := string(value)

	if len(keyString) == 0 {
		return "", fmt.Errorf("Entity key has zero length")
	}

	// Check the prefix
	if string(keyString[0]) != entityKeyPrefix {
		return "", fmt.Errorf("Entity key %v has the wrong prefix", keyString)
	}

	// Return the entity ID by removing the prefix
	return keyString[1:], nil
}

// bipartiteEntityToPebbleValue converts an entity to a Pebble value.
func bipartiteEntityToPebbleValue(entity *Entity) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(entity); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToBipartiteEntity converts a Pebble value to an entity.
func pebbleValueToBipartiteEntity(value []byte) (*Entity, error) {
	var buffer bytes.Buffer
	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	var entity Entity
	if err := decoder.Decode(&entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// bipartiteDocumentIdToPebbleKey converts a document ID to a Pebble key.
func bipartiteDocumentIdToPebbleKey(id string) []byte {
	return []byte(documentKeyPrefix + id)
}

// pebbleKeyToBipartiteDocumentId converts a Pebble key to a document ID.
func pebbleKeyToBipartiteDocumentId(value []byte) (string, error) {

	// Preconditions
	if value == nil {
		return "", fmt.Errorf("Entity key is nil")
	}

	keyString := string(value)

	if len(keyString) == 0 {
		return "", fmt.Errorf("Entity key has zero length")
	}

	// Check the prefix
	if string(keyString[0]) != documentKeyPrefix {
		return "", fmt.Errorf("Document key %v has the wrong prefix", keyString)
	}

	// Return the entity ID by removing the prefix
	return keyString[1:], nil
}

func bipartiteDocumentToPebbleValue(document *Document) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(document); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToBipartiteDocument converts a Pebble value to a document.
func pebbleValueToBipartiteDocument(value []byte) (*Document, error) {
	var buffer bytes.Buffer
	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	var document Document
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}

	return &document, nil
}

// AddEntity to the Pebble store.
func (p *PebbleBipartiteGraphStore) AddEntity(entity Entity) error {

	// Preconditions
	if len(entity.Id) == 0 {
		return fmt.Errorf("Entity ID is empty")
	}

	// Pebble key for the entity
	pebbleKey := bipartiteEntityIdToPebbleKey(entity.Id)

	// Pebble value for the entity
	pebbleValue, err := bipartiteEntityToPebbleValue(&entity)
	if err != nil {
		return err
	}

	return p.db.Set(pebbleKey, pebbleValue, pebble.Sync)
}

var (
	ErrEntityNotFound   = fmt.Errorf("Entity not found")
	ErrDocumentNotFound = fmt.Errorf("Document not found")
)

func (p *PebbleBipartiteGraphStore) GetEntity(entityId string) (*Entity, error) {

	// Preconditions
	if len(entityId) == 0 {
		return nil, fmt.Errorf("Empty entity ID")
	}

	value, closer, err := p.db.Get(bipartiteEntityIdToPebbleKey(entityId))

	if err == pebble.ErrNotFound {
		return nil, ErrEntityNotFound
	}

	if err != nil {
		return nil, err
	}

	if err2 := closer.Close(); err2 != nil {
		return nil, err2
	}

	// Convert the Pebble value (bytes) to an Entity
	return pebbleValueToBipartiteEntity(value)
}

// AddDocument to the store.
func (p *PebbleBipartiteGraphStore) AddDocument(document Document) error {

	// Preconditions
	if len(document.Id) == 0 {
		return fmt.Errorf("Document has an empty ID")
	}

	pebbleKey := bipartiteDocumentIdToPebbleKey(document.Id)

	pebbleValue, err := bipartiteDocumentToPebbleValue(&document)
	if err != nil {
		return err
	}

	return p.db.Set(pebbleKey, pebbleValue, pebble.Sync)
}

// GetDocument from the Pebble store given its ID.
func (p *PebbleBipartiteGraphStore) GetDocument(documentId string) (*Document, error) {

	// Preconditions
	if len(documentId) == 0 {
		return nil, fmt.Errorf("Document ID is empty")
	}

	value, closer, err := p.db.Get(bipartiteDocumentIdToPebbleKey(documentId))

	if err == pebble.ErrNotFound {
		return nil, ErrDocumentNotFound
	}

	if err != nil {
		return nil, err
	}

	if err2 := closer.Close(); err2 != nil {
		return nil, err2
	}

	// Convert the Pebble value (bytes) to an Entity
	return pebbleValueToBipartiteDocument(value)
}

// A BipartiteGraphStore holds entities and documents.
// type BipartiteGraphStore interface {
// 	AddLink(Link) error                        // Add a link from an entity to a document (by ID)
// 	Clear() error                              // Clear the store
// 	Equal(BipartiteGraphStore) bool            // Do two stores have the same contents?
// 	GetDocument(string) *Document              // Get a document by document ID
// 	HasDocument(*Document) bool                // Does the graph store contain the document?
// 	HasEntity(*Entity) bool                    // Does the graph store contain the entity?
// 	NewDocumentIdIterator() DocumentIdIterator // Get a document ID iterator
// 	NewEntityIdIterator() EntityIdIterator     // Get an entity ID iterator
// 	NumberOfEntities() int                     // Number of entities in the store
// 	NumberOfDocuments() int                    // Number of documents in the store
// }
