// This implementation of the bipartite graph store uses Pebble DB as the backend. The bipartite
// graph is composed of entities and documents.
//
// Entities are stored as:
//
//   e#<entity ID> = <serialised entity>
//
// Documents are stored as:
//
//   d#<document ID> = <serialised document>
//
// Entity-document links are stored as:
//
//   edl#<entity ID>#<document ID> = nil
//
// Document-entity links are stored as:
//
//   del#<document ID>#<entity ID> = nil

package graphstore

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	entityPrefix             = "e"
	documentPrefix           = "d"
	entityDocumentLinkPrefix = "edl"
	documentEntityLinkPrefix = "del"
)

var (
	ErrMalformedEntityKey                 = errors.New("malformed entity key")
	ErrMalformedDocumentKey               = errors.New("malformed document key")
	ErrPebbleKeyIsNil                     = errors.New("pebble key is nil")
	ErrPebbleValueIsNil                   = errors.New("pebble value is nil")
	ErrEmptyDocumentId                    = errors.New("empty document ID")
	ErrDocumentIdContainsIllegalCharacter = errors.New("document ID contains illegal character")
)

// A PebbleBipartiteGraphStore is a bipartite graph store backed by the Pebble key-value database.
type PebbleBipartiteGraphStore struct {
	folder string
	db     *pebble.DB
}

type PebbleEntity struct {
	Id         string
	EntityType string
	Attributes map[string]string
}

func EntityToPebbleEntity(e Entity) PebbleEntity {
	return PebbleEntity{
		Id:         e.Id,
		EntityType: e.EntityType,
		Attributes: e.Attributes,
	}
}

func PebbleEntityToEntity(pebbleEntity PebbleEntity, documents *set.Set[string]) Entity {
	return Entity{
		Id:                pebbleEntity.Id,
		EntityType:        pebbleEntity.EntityType,
		Attributes:        pebbleEntity.Attributes,
		LinkedDocumentIds: documents,
	}
}

type PebbleDocument struct {
	Id           string
	DocumentType string
	Attributes   map[string]string
}

func DocumentToPebbleDocument(d Document) PebbleDocument {
	return PebbleDocument{
		Id:           d.Id,
		DocumentType: d.DocumentType,
		Attributes:   d.Attributes,
	}
}

func PebbleDocumentToDocument(pebbleDocument PebbleDocument, entities *set.Set[string]) Document {
	return Document{
		Id:              pebbleDocument.Id,
		DocumentType:    pebbleDocument.DocumentType,
		Attributes:      pebbleDocument.Attributes,
		LinkedEntityIds: entities,
	}
}

// NewPebbleBipartiteGraphStore given the dedicated folder where the Pebble files are to be held.
func NewPebbleBipartiteGraphStore(folder string) (*PebbleBipartiteGraphStore, error) {

	if len(folder) == 0 {
		return nil, errors.New("folder name is empty")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", folder).
		Msg("Opening bipartite Pebble store")

	db, err := pebble.Open(folder, &pebble.Options{
		FS: vfs.Default,
	})
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

func (p *PebbleBipartiteGraphStore) Finalise() error {
	return p.db.Flush()
}

// entityIdToPebbleKey generates the Pebble key for an entity ID.
func entityIdToPebbleKey(id string) ([]byte, error) {

	if err := validateEntityId(id); err != nil {
		return nil, err
	}

	return []byte(entityPrefix + separator + id), nil
}

// pebbleKeyToEntityId extracts the entity ID from the Pebble key.
func pebbleKeyToEntityId(key []byte) (string, error) {

	if key == nil {
		return "", ErrPebbleKeyIsNil
	}

	parts := strings.Split(string(key), separator)

	if len(parts) != 2 {
		return "", fmt.Errorf("%w: %v", ErrMalformedEntityKey, string(key))
	}

	entityId := parts[1]
	if err := validateEntityId(entityId); err != nil {
		return "", err
	}

	return entityId, nil
}

// entityToPebbleValue converts a Pebble entity to a value for the key-value store.
func entityToPebbleValue(entity *PebbleEntity) ([]byte, error) {

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(entity); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToEntity converts a Pebble value to a Pebble entity.
func pebbleValueToEntity(value []byte) (*PebbleEntity, error) {

	if value == nil {
		return nil, ErrPebbleValueIsNil
	}

	var buffer bytes.Buffer
	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	var entity PebbleEntity
	if err := decoder.Decode(&entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// validateDocumentId validates the document ID prior to storage.
func validateDocumentId(id string) error {

	if len(id) == 0 {
		return ErrEmptyDocumentId
	}

	if strings.Contains(id, separator) {
		return ErrDocumentIdContainsIllegalCharacter
	}

	return nil
}

// documentIdToPebbleKey generates the Pebble key for an document ID.
func documentIdToPebbleKey(id string) ([]byte, error) {

	if err := validateDocumentId(id); err != nil {
		return nil, err
	}

	return []byte(documentPrefix + separator + id), nil
}

// pebbleKeyToDocumentId extracts the document ID from the Pebble key.
func pebbleKeyToDocumentId(key []byte) (string, error) {

	if key == nil {
		return "", ErrPebbleKeyIsNil
	}

	parts := strings.Split(string(key), separator)

	if len(parts) != 2 {
		return "", fmt.Errorf("%w: %v", ErrMalformedDocumentKey, string(key))
	}

	documentId := parts[1]
	if err := validateDocumentId(documentId); err != nil {
		return "", err
	}

	return documentId, nil
}

// documentToPebbleValue returns the value for a Pebble document.
func documentToPebbleValue(document *PebbleDocument) ([]byte, error) {

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(document); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToDocument returns a Pebble document held in a Pebble value.
func pebbleValueToDocument(value []byte) (*PebbleDocument, error) {

	if value == nil {
		return nil, ErrPebbleValueIsNil
	}

	var buffer bytes.Buffer
	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	var entity PebbleDocument
	if err := decoder.Decode(&entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// entityDocumentLinkToPebbleKey returns the pebble key for an entity-document link.
func entityDocumentLinkToPebbleKey(entityId string, documentId string) ([]byte, error) {

	if err := validateEntityId(entityId); err != nil {
		return nil, err
	}

	if err := validateDocumentId(documentId); err != nil {
		return nil, err
	}

	return []byte(entityDocumentLinkPrefix + separator + entityId + separator + documentId), nil
}

// pebbleKeyToEntityDocumentLink returns the entity and document IDs for a Pebble key.
func pebbleKeyToEntityDocumentLink(key []byte) (string, string, error) {

	if key == nil {
		return "", "", ErrPebbleKeyIsNil
	}

	parts := strings.Split(string(key), separator)

	if len(parts) != 3 || parts[0] != entityDocumentLinkPrefix {
		return "", "", fmt.Errorf("%w: %v", ErrMalformedDocumentKey, string(key))
	}

	entityId := parts[1]
	documentId := parts[2]

	if err := validateEntityId(entityId); err != nil {
		return "", "", err
	}

	if err := validateDocumentId(documentId); err != nil {
		return "", "", err
	}

	return entityId, documentId, nil
}

// documentEntityLinkToPebbleKey returns the Pebble key for a document-entity link.
func documentEntityLinkToPebbleKey(documentId string, entityId string) ([]byte, error) {

	if err := validateDocumentId(documentId); err != nil {
		return nil, err
	}

	if err := validateEntityId(entityId); err != nil {
		return nil, err
	}

	return []byte(documentEntityLinkPrefix + separator + documentId + separator + entityId), nil
}

// pebbleKeyToDocumentEntityLink converts a Pebble key to a document-entity link.
func pebbleKeyToDocumentEntityLink(key []byte) (string, string, error) {

	if key == nil {
		return "", "", ErrPebbleKeyIsNil
	}

	parts := strings.Split(string(key), separator)

	if len(parts) != 3 || parts[0] != documentEntityLinkPrefix {
		return "", "", fmt.Errorf("%w: %v", ErrMalformedDocumentKey, string(key))
	}

	documentId := parts[1]
	entityId := parts[2]

	if err := validateDocumentId(documentId); err != nil {
		return "", "", err
	}

	if err := validateEntityId(entityId); err != nil {
		return "", "", err
	}

	return documentId, entityId, nil
}

func (p *PebbleBipartiteGraphStore) putEntityDocumentLink(entityId string, documentId string) error {

	// Store the entity -> document link
	key, err := entityDocumentLinkToPebbleKey(entityId, documentId)
	if err != nil {
		return err
	}

	return p.db.Set(key, nil, pebble.NoSync)
}

func (p *PebbleBipartiteGraphStore) putDocumentEntityLink(documentId string, entityId string) error {
	// Store the entity <- document link
	key, err := documentEntityLinkToPebbleKey(documentId, entityId)
	if err != nil {
		return err
	}

	return p.db.Set(key, nil, pebble.NoSync)
}

func (p *PebbleBipartiteGraphStore) putEntitiesForDocument(docId string, entities *set.Set[string]) error {

	for _, entityId := range entities.ToSlice() {
		if err := p.putDocumentEntityLink(docId, entityId); err != nil {
			return err
		}
	}

	return nil
}

func (p *PebbleBipartiteGraphStore) getEntitiesForDocument(docId string) (*set.Set[string], error) {

	entityIds := set.NewSet[string]()

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(documentEntityLinkPrefix + separator + docId + separator),
		UpperBound: []byte(documentEntityLinkPrefix + separator + docId + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	var errDuringIteration error
	for iter.First(); iter.Valid() && errDuringIteration == nil; iter.Next() {

		retrievedDocId, entityId, err := pebbleKeyToDocumentEntityLink(iter.Key())

		if err != nil {
			errDuringIteration = err
		} else if retrievedDocId != docId {
			errDuringIteration = ErrMalformedKey
		} else {
			entityIds.Add(entityId)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	if errDuringIteration != nil {
		return nil, errDuringIteration
	}

	return entityIds, nil
}

func (p *PebbleBipartiteGraphStore) putDocumentsForEntity(entityId string, documents *set.Set[string]) error {

	for _, docId := range documents.ToSlice() {
		if err := p.putEntityDocumentLink(entityId, docId); err != nil {
			return err
		}
	}

	return nil
}

func (p *PebbleBipartiteGraphStore) getDocumentsForEntity(entityId string) (*set.Set[string], error) {

	documentIds := set.NewSet[string]()

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(entityDocumentLinkPrefix + separator + entityId + separator),
		UpperBound: []byte(entityDocumentLinkPrefix + separator + entityId + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	var errDuringIteration error
	for iter.First(); iter.Valid() && errDuringIteration == nil; iter.Next() {

		retrievedEntityId, documentId, err := pebbleKeyToEntityDocumentLink(iter.Key())

		if err != nil {
			errDuringIteration = err
		} else if retrievedEntityId != entityId {
			errDuringIteration = ErrMalformedKey
		} else {
			documentIds.Add(documentId)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	if errDuringIteration != nil {
		return nil, errDuringIteration
	}

	return documentIds, nil
}

func (p *PebbleBipartiteGraphStore) putPebbleEntity(entity PebbleEntity) error {

	// Make the key
	key, err := entityIdToPebbleKey(entity.Id)
	if err != nil {
		return err
	}

	// Make the value
	value, err := entityToPebbleValue(&entity)
	if err != nil {
		return err
	}

	// Store
	return p.db.Set(key, value, pebble.NoSync)
}

// AddEntity to the Pebble store.
func (p *PebbleBipartiteGraphStore) AddEntity(entity Entity) error {

	// Convert the entity to a Pebble entity
	pebbleEntity := EntityToPebbleEntity(entity)

	// Store the Pebble entity
	if err := p.putPebbleEntity(pebbleEntity); err != nil {
		return err
	}

	// Store the associated documents
	return p.putDocumentsForEntity(entity.Id, entity.LinkedDocumentIds)
}

func (p *PebbleBipartiteGraphStore) putPebbleDocument(document PebbleDocument) error {

	// Make the key
	key, err := documentIdToPebbleKey(document.Id)
	if err != nil {
		return err
	}

	// Make the value
	value, err := documentToPebbleValue(&document)
	if err != nil {
		return err
	}

	// Store
	return p.db.Set(key, value, pebble.NoSync)
}

// AddDocument to the Pebble store.
func (p *PebbleBipartiteGraphStore) AddDocument(document Document) error {

	// Convert the document to a Pebble document
	pebbleDocument := DocumentToPebbleDocument(document)

	// Store the Pebble document
	if err := p.putPebbleDocument(pebbleDocument); err != nil {
		return err
	}

	// Store the associated entities
	return p.putEntitiesForDocument(document.Id, document.LinkedEntityIds)
}

// AddLink between an entity and a document (by ID).
func (p *PebbleBipartiteGraphStore) AddLink(link Link) error {

	err := p.putEntityDocumentLink(link.EntityId, link.DocumentId)
	if err != nil {
		return err
	}

	return p.putDocumentEntityLink(link.DocumentId, link.EntityId)
}

// GetEntity given its ID from the Pebble store.
func (p *PebbleBipartiteGraphStore) GetEntity(entityId string) (*Entity, error) {

	// Get the entity from the Pebble store
	key, err := entityIdToPebbleKey(entityId)
	if err != nil {
		return nil, err
	}

	value, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrEntityNotFound
		}
		return nil, err
	}

	defer closer.Close()

	entity, err := pebbleValueToEntity(value)
	if err != nil {
		return nil, err
	}

	// Get the documents for the entity
	docs, err := p.getDocumentsForEntity(entityId)
	if err != nil {
		return nil, err
	}

	ent := PebbleEntityToEntity(*entity, docs)

	return &ent, nil
}

// GetDocument from the Pebble store given its ID.
func (p *PebbleBipartiteGraphStore) GetDocument(documentId string) (*Document, error) {

	// Get the document from the Pebble store
	key, err := documentIdToPebbleKey(documentId)
	if err != nil {
		return nil, err
	}

	value, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrDocumentNotFound
		}
		return nil, err
	}

	defer closer.Close()

	document, err := pebbleValueToDocument(value)
	if err != nil {
		return nil, err
	}

	// Got the entities for the document
	entities, err := p.getEntitiesForDocument(documentId)
	if err != nil {
		return nil, err
	}

	doc := PebbleDocumentToDocument(*document, entities)

	return &doc, nil
}

// HasDocument returns true if the store contains the document.
func (p *PebbleBipartiteGraphStore) HasDocument(document *Document) (bool, error) {

	// Get the document from the store
	doc, err := p.GetDocument(document.Id)
	if err == ErrDocumentNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return doc.Equal(document), nil
}

// HasEntity returns true if the entity exists in the Pebble store.
func (p *PebbleBipartiteGraphStore) HasEntity(entity *Entity) (bool, error) {

	// Get the entity from the store
	ent, err := p.GetEntity(entity.Id)
	if err == ErrEntityNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return ent.Equal(entity), nil
}

func (p *PebbleBipartiteGraphStore) HasEntityWithId(entityId string) (bool, error) {

	key, err := entityIdToPebbleKey(entityId)
	if err != nil {
		return false, err
	}

	_, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}

	err = closer.Close()
	if err != nil {
		return false, err
	}

	return true, nil
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
		nextDocumentId, err = pebbleKeyToDocumentId(key)
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

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(documentPrefix + separator),
		UpperBound: []byte(documentPrefix + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	iter.First()

	var docId string
	var err error

	documentIdIterator := PebbleDocumentIterator{
		iter: iter,
	}

	if iter.Valid() {
		pebbleKey := iter.Key()
		docId, err = pebbleKeyToDocumentId(pebbleKey)

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
		nextEntityId, err = pebbleKeyToEntityId(key)
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

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(entityPrefix + separator),
		UpperBound: []byte(entityPrefix + separatorPlusOne),
	}
	iter := p.db.NewIter(iterOptions)
	iter.First()

	var entityId string
	var err error

	entityIdIterator := PebbleEntityIterator{
		iter: iter,
	}

	if iter.Valid() {
		pebbleKey := iter.Key()
		entityId, err = pebbleKeyToEntityId(pebbleKey)

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
