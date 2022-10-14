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
	"os"

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

// bipartiteDocumentToPebbleValue converts a document to a Pebble value.
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
		return ErrEntityIdIsEmpty
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

func (p *PebbleBipartiteGraphStore) GetEntity(entityId string) (*Entity, error) {

	// Preconditions
	if len(entityId) == 0 {
		return nil, ErrEntityIdIsEmpty
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

func (p *PebbleBipartiteGraphStore) HasEntity(entity *Entity) (bool, error) {

	// Preconditions
	if entity == nil {
		return false, ErrEntityIsNil
	}

	if len(entity.Id) == 0 {
		return false, ErrEntityIdIsEmpty
	}

	ent, err := p.GetEntity(entity.Id)

	if err == ErrEntityNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return ent.Equal(entity), nil
}

// AddDocument to the store.
func (p *PebbleBipartiteGraphStore) AddDocument(document Document) error {

	// Preconditions
	if len(document.Id) == 0 {
		return ErrEntityIdIsEmpty
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
		return nil, ErrDocumentIdIsEmpty
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

// HasDocument returns true if the store contains the document.
func (p *PebbleBipartiteGraphStore) HasDocument(document *Document) (bool, error) {

	// Preconditions
	if document == nil {
		return false, ErrDocumentIsNil
	}

	if len(document.Id) == 0 {
		return false, ErrDocumentIdIsEmpty
	}

	doc, err := p.GetDocument(document.Id)

	if err == ErrDocumentNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return document.Equal(doc), nil
}

// AddLink between an entity and a document (by ID).
func (p *PebbleBipartiteGraphStore) AddLink(link Link) error {

	// Preconditions
	if len(link.EntityId) == 0 {
		return ErrEntityIdIsEmpty
	} else if len(link.DocumentId) == 0 {
		return ErrDocumentIdIsEmpty
	}

	// Get the document from the store
	document, err := p.GetDocument(link.DocumentId)
	if err != nil {
		return err
	}

	// Get the entity from the store
	entity, err := p.GetEntity(link.EntityId)
	if err != nil {
		return err
	}

	// Add the link from the entity to the document
	document.AddEntity(link.EntityId)

	// Add the link from the document to the entity
	entity.AddDocument(link.DocumentId)

	// Store the modified entity
	if err := p.AddEntity(*entity); err != nil {
		return err
	}

	// Store the modified document
	if err := p.AddDocument(*document); err != nil {
		return err
	}

	return nil
}

func makePebbleKeyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func makePebblePrefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: makePebbleKeyUpperBound(prefix),
	}
}

type PebbleDocumentIterator struct {
	iter      *pebble.Iterator
	currentId string
	hasNextId bool
}

func (it *PebbleDocumentIterator) nextDocumentId() (string, error) {

	// Is there another entry in the Pebble iterator?
	isNext := it.iter.Next()

	var err error
	var nextDocumentId string

	// If there aren't any more documents, close the iterator
	if !isNext {
		err = it.close()
		it.hasNextId = false
	} else {
		it.hasNextId = true
		key := it.iter.Key() // Next Pebble key
		nextDocumentId, err = pebbleKeyToBipartiteDocumentId(key)
	}

	toReturn := it.currentId
	it.currentId = nextDocumentId

	return toReturn, err
}

func (it *PebbleDocumentIterator) hasNext() bool {
	return it.hasNextId
}

func (it *PebbleDocumentIterator) close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

// NewDocumentIdIterator returns a document ID iterator.
func (p *PebbleBipartiteGraphStore) NewDocumentIdIterator() (DocumentIdIterator, error) {

	documentKey := []byte(documentKeyPrefix)
	iter := p.db.NewIter(makePebblePrefixIterOptions(documentKey))
	iter.First()

	var docId string
	var err error

	documentIdIterator := PebbleDocumentIterator{
		iter: iter,
	}

	if iter.Valid() {
		pebbleKey := iter.Key()
		docId, err = pebbleKeyToBipartiteDocumentId(pebbleKey)

		documentIdIterator.currentId = docId
		documentIdIterator.hasNextId = true
	} else {
		documentIdIterator.hasNextId = false
		err = documentIdIterator.close()
	}

	if err != nil {
		documentIdIterator.close()
	}

	return &documentIdIterator, err
}

func (p *PebbleBipartiteGraphStore) NumberOfDocuments() (int, error) {
	nDocuments := 0

	iter, err := p.NewDocumentIdIterator()
	if err != nil {
		return 0, err
	}

	for iter.hasNext() {
		_, err := iter.nextDocumentId()
		if err != nil {
			return 0, err
		}
		nDocuments += 1
	}

	return nDocuments, nil
}

type PebbleEntityIterator struct {
	iter      *pebble.Iterator
	currentId string
	hasNextId bool
}

func (it *PebbleEntityIterator) nextEntityId() (string, error) {

	// Is there another entry in the Pebble iterator?
	isNext := it.iter.Next()

	var err error
	var nextEntityId string

	// If there aren't any more entities, close the iterator
	if !isNext {
		err = it.close()
		it.hasNextId = false
	} else {
		it.hasNextId = true
		key := it.iter.Key() // Next Pebble key
		nextEntityId, err = pebbleKeyToBipartiteEntityId(key)
	}

	toReturn := it.currentId
	it.currentId = nextEntityId

	return toReturn, err
}

func (it *PebbleEntityIterator) hasNext() bool {
	return it.hasNextId
}

func (it *PebbleEntityIterator) close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

// NewDocumentIdIterator returns a document ID iterator.
func (p *PebbleBipartiteGraphStore) NewEntityIdIterator() (EntityIdIterator, error) {

	entityKey := []byte(entityKeyPrefix)
	iter := p.db.NewIter(makePebblePrefixIterOptions(entityKey))
	iter.First()

	var entityId string
	var err error

	entityIdIterator := PebbleEntityIterator{
		iter: iter,
	}

	if iter.Valid() {
		pebbleKey := iter.Key()
		entityId, err = pebbleKeyToBipartiteEntityId(pebbleKey)

		entityIdIterator.currentId = entityId
		entityIdIterator.hasNextId = true
	} else {
		entityIdIterator.hasNextId = false
		err = entityIdIterator.close()
	}

	if err != nil {
		entityIdIterator.close()
	}

	return &entityIdIterator, err
}

func (p *PebbleBipartiteGraphStore) NumberOfEntities() (int, error) {
	nEntities := 0

	iter, err := p.NewEntityIdIterator()
	if err != nil {
		return 0, err
	}

	for iter.hasNext() {
		_, err := iter.nextEntityId()
		if err != nil {
			return 0, err
		}
		nEntities += 1
	}

	return nEntities, nil
}

// Equal returns true if two stores have the same contents.
func (p *PebbleBipartiteGraphStore) Equal(other BipartiteGraphStore) (bool, error) {
	return bipartiteGraphStoresEqual(p, other)
}

// Clear the store.
func (p *PebbleBipartiteGraphStore) Clear() error {

	var deleteError error

	// As soon as there is an error when deleting a key, stop the iteration
	// close the iterator (to prevent a memory leak) and return
	iter := p.db.NewIter(nil)
	for iter.First(); iter.Valid() && deleteError == nil; iter.Next() {
		key := iter.Key()
		deleteError = p.db.Delete(key, pebble.Sync)
	}

	if err := iter.Close(); err != nil {
		return err
	}

	return deleteError
}

// Destroy the bipartite Pebble store after closing the database.
func (p *PebbleBipartiteGraphStore) Destroy() error {

	// Close down the Pebble database
	err := p.Close()
	if err != nil {
		return err
	}

	// Delete the folder
	return os.RemoveAll(p.folder)
}
