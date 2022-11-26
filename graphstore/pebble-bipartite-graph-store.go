// This implementation of the bipartite graph store uses Pebble DB as the backend. The bipartite
// graph is composed of entities and documents.
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
	"errors"
	"fmt"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cockroachdb/pebble"
)

// A PebbleBipartiteGraphStore is a bipartite graph store backed by the Pebble key-value database.
type PebbleBipartiteGraphStore struct {
	folder string
	db     *pebble.DB
}

// NewPebbleBipartiteGraphStore given the dedicated folder where the Pebble files are to be held.
func NewPebbleBipartiteGraphStore(folder string) (*PebbleBipartiteGraphStore, error) {

	// Preconditions
	if len(folder) == 0 {
		return nil, errors.New("folder name is empty")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", folder).
		Msg("Creating a new bipartite Pebble store")

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

// Close the Pebble store.
func (p *PebbleBipartiteGraphStore) Close() error {
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Closing the Pebble bipartite graph store")

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
		return "", errors.New("entity key is nil")
	}

	keyString := string(value)

	if len(keyString) == 0 {
		return "", errors.New("entity key has zero length")
	}

	// Check the prefix
	if string(keyString[0]) != entityKeyPrefix {
		return "", fmt.Errorf("entity key %v has the wrong prefix", keyString)
	}

	// Return the entity ID by removing the prefix
	return keyString[1:], nil
}

// bipartiteEntityToPebbleValue converts an entity to a Pebble value.
func bipartiteEntityToPebbleValue(entity *Entity) ([]byte, error) {

	// Preconditions
	if entity == nil {
		return nil, errors.New("entity is nil")
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(entity); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToBipartiteEntity converts a Pebble value to an entity.
func pebbleValueToBipartiteEntity(value []byte) (*Entity, error) {

	// Preconditions
	if value == nil {
		return nil, errors.New("pebble value is nil")
	}

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
		return "", errors.New("document key is nil")
	}

	keyString := string(value)

	if len(keyString) == 0 {
		return "", errors.New("document key has zero length")
	}

	// Check the prefix
	if string(keyString[0]) != documentKeyPrefix {
		return "", fmt.Errorf("document key %v has the wrong prefix", keyString)
	}

	// Return the entity ID by removing the prefix
	return keyString[1:], nil
}

// bipartiteDocumentToPebbleValue converts a document to a Pebble value.
func bipartiteDocumentToPebbleValue(document *Document) ([]byte, error) {

	// Preconditions
	if document == nil {
		return nil, fmt.Errorf("Document is nil")
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(document); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToBipartiteDocument converts a Pebble value to a document.
func pebbleValueToBipartiteDocument(value []byte) (*Document, error) {

	// Preconditions
	if value == nil {
		return nil, errors.New("pebble value is nil")
	}

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
	err := ValidateEntityId(entity.Id)
	if err != nil {
		return ErrEntityIdIsEmpty
	}

	// Pebble key for the entity
	pebbleKey := bipartiteEntityIdToPebbleKey(entity.Id)

	// Pebble value for the entity
	pebbleValue, err := bipartiteEntityToPebbleValue(&entity)
	if err != nil {
		return err
	}

	return p.db.Set(pebbleKey, pebbleValue, pebble.NoSync)
}

// GetEntity given its ID from the Pebble store.
func (p *PebbleBipartiteGraphStore) GetEntity(entityId string) (*Entity, error) {

	// Preconditions
	err := ValidateEntityId(entityId)
	if err != nil {
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

// HasEntity returns true if the entity exists in the Pebble store.
func (p *PebbleBipartiteGraphStore) HasEntity(entity *Entity) (bool, error) {

	// Preconditions
	if entity == nil {
		return false, ErrEntityIsNil
	}

	err := ValidateEntityId(entity.Id)
	if err != nil {
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

// AddDocument to the Pebble store.
func (p *PebbleBipartiteGraphStore) AddDocument(document Document) error {

	// Preconditions
	err := ValidateDocumentId(document.Id)
	if err != nil {
		return ErrDocumentIdIsEmpty
	}

	pebbleKey := bipartiteDocumentIdToPebbleKey(document.Id)

	pebbleValue, err := bipartiteDocumentToPebbleValue(&document)
	if err != nil {
		return err
	}

	return p.db.Set(pebbleKey, pebbleValue, pebble.NoSync)
}

// GetDocument from the Pebble store given its ID.
func (p *PebbleBipartiteGraphStore) GetDocument(documentId string) (*Document, error) {

	// Preconditions
	err := ValidateDocumentId(documentId)
	if err != nil {
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

	err := ValidateDocumentId(document.Id)
	if err != nil {
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
	err := ValidateEntityId(link.EntityId)
	if err != nil {
		return ErrEntityIdIsEmpty
	}

	err = ValidateDocumentId(link.DocumentId)
	if err != nil {
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

// makePebbleKeyUpperBound constructs the upper bound of the key for scanning.
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

// makePebblePrefixIterOptions constructs an IterOptions for scanning all entries with a given
// prefix.
func makePebblePrefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: makePebbleKeyUpperBound(prefix),
	}
}

// PebbleDocumentIterator is an iterator for walking through all Documents in the Pebble store.
type PebbleDocumentIterator struct {
	iter      *pebble.Iterator // Pebble iterator
	currentId string           // Current Document ID
	hasNextId bool             // Is there another Document ID?
}

// nextDocumentId gets the next Document ID from the iterator.
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

// hasNext returns true if there is another Document ID available.
func (it *PebbleDocumentIterator) hasNext() bool {
	return it.hasNextId
}

// close the iterator.
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

// NumberOfDocuments in the Pebble bipartite store.
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

// A PebbleEntityIterator is for walking through all Entities in the Pebble store.
type PebbleEntityIterator struct {
	iter      *pebble.Iterator // Pebble iterator
	currentId string           // Current Entity ID
	hasNextId bool             // Is there another Entity ID?
}

// nextEntityId from the iterator.
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

// hasNext returns true if the iterator is not exhausted.
func (it *PebbleEntityIterator) hasNext() bool {
	return it.hasNextId
}

// close the iterator.
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

// NumberOfEntities in the bipartite Pebble store.
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

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Clearing the Pebble bipartite graph store")

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

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Destroying the Pebble bipartite graph store")

	// Close down the Pebble database
	err := p.Close()
	if err != nil {
		return err
	}

	// Delete the folder
	return os.RemoveAll(p.folder)
}
