// A Pebble unipartite graph store is a database-backed component for holding a graph structure
// of a single type of node. Pebble DB is a key-value store and it provides the persistence.
//
// A new Pebble store is created using the NewPebbleUnipartiteGraphStore() function. Note that
// the function opens the database connection ready for reading. Once the database is no longer
// needed (e.g. because the program using the database is closing down), the Close() method must be
// called.

package graphstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cockroachdb/pebble"
)

// A PebbleUnipartiteGraphStore is a Pebble-backed unipartite graph store.
type PebbleUnipartiteGraphStore struct {
	folder string     // Folder for the Pebble files
	db     *pebble.DB // Pebble database
}

// NewPebbleUnipartiteGraphStore given the folder in which to store the Pebble files.
func NewPebbleUnipartiteGraphStore(folder string) (*PebbleUnipartiteGraphStore, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", folder).
		Msg("Creating a new unipartite Pebble store")

	db, err := pebble.Open(folder, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	store := PebbleUnipartiteGraphStore{
		folder: folder,
		db:     db,
	}

	return &store, nil
}

// Close the Pebble store.
func (p *PebbleUnipartiteGraphStore) Close() error {
	return p.db.Close()
}

// entityIdToPebbleKey converts the entity ID to a Pebble key.
func entityIdToPebbleKey(id string) []byte {
	return []byte(id)
}

// pebbleKeyToEntityId converts a Pebble key to an entity ID.
func pebbleKeyToEntityId(value []byte) string {
	return string(value)
}

// entityIdsToPebbleValue converts a set of entity IDs to a Pebble value.
func entityIdsToPebbleValue(s *set.Set[string]) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(s); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// pebbleValueToEntityIds converts a Pebble value to a set of entity IDs.
func pebbleValueToEntityIds(value []byte) (*set.Set[string], error) {
	var buffer bytes.Buffer
	buffer.Write(value)
	decoder := gob.NewDecoder(&buffer)

	var s set.Set[string]
	if err := decoder.Decode(&s); err != nil {
		return nil, err
	}

	return &s, nil
}

// dstEntityIds returns the destination entity IDs for a given source entity.
func (p *PebbleUnipartiteGraphStore) dstEntityIds(src string) (*set.Set[string], bool, error) {

	value, closer, err := p.db.Get(entityIdToPebbleKey(src))

	if err == pebble.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	if err2 := closer.Close(); err2 != nil {
		return nil, false, err2
	}

	set, err := pebbleValueToEntityIds(value)

	return set, true, err
}

// setSrcToDsts sets the source entity to destination entity connections.
func (p *PebbleUnipartiteGraphStore) setSrcToDsts(src string, dsts *set.Set[string]) error {

	key := entityIdToPebbleKey(src)

	value, err := entityIdsToPebbleValue(dsts)
	if err != nil {
		return err
	}

	return p.db.Set(key, value, pebble.NoSync)
}

// HasEntity returns true if the entity ID is held within the backend.
func (p *PebbleUnipartiteGraphStore) HasEntity(id string) (bool, error) {

	_, found, err := p.dstEntityIds(id)
	return found, err
}

// AddEntity to the unipartite graph store.
func (p *PebbleUnipartiteGraphStore) AddEntity(id string) error {

	// Preconditions
	err := ValidateEntityId(id)
	if err != nil {
		return ErrEntityIdIsBlank
	}

	found, err := p.HasEntity(id)
	if err != nil {
		return err
	}

	// If the entity doesn't already exist in the backend, then add it
	if !found {
		return p.setSrcToDsts(id, set.NewSet[string]())
	}

	return nil
}

// AddDirected edge between the source (src) and destination (dst) vertices.
func (p *PebbleUnipartiteGraphStore) AddDirected(src string, dst string) error {

	// Preconditions
	err := ValidateEntityId(src)
	if err != nil {
		return ErrEntityIdIsBlank
	}

	err = ValidateEntityId(dst)
	if err != nil {
		return ErrEntityIdIsBlank
	}

	if src == dst {
		return fmt.Errorf("source and destination IDs are identical (%v)", src)
	}

	existingSet, found, err := p.dstEntityIds(src)

	if err != nil {
		return err
	}

	if found {
		existingSet.Add(dst)
	} else {
		existingSet = set.NewPopulatedSet(dst)
	}

	return p.setSrcToDsts(src, existingSet)
}

// AddUndirected edge between two entities.
func (p *PebbleUnipartiteGraphStore) AddUndirected(src string, dst string) error {

	// Preconditions
	err := ValidateEntityId(src)
	if err != nil {
		return ErrEntityIdIsBlank
	}

	err = ValidateEntityId(dst)
	if err != nil {
		return ErrEntityIdIsBlank
	}

	if src == dst {
		return fmt.Errorf("source and destination IDs are identical (%v)", src)
	}

	// Add the src --> dst connection
	err = p.AddDirected(src, dst)
	if err != nil {
		return err
	}

	// Add the src <-- dst connection
	return p.AddDirected(dst, src)
}

// Clear down the graph.
func (p *PebbleUnipartiteGraphStore) Clear() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Clearing the Pebble unipartite store")

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

// Destroy the unipartite Pebble store after closing the database.
func (p *PebbleUnipartiteGraphStore) Destroy() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Destroying the Pebble unipartite store")

	err := p.Close()
	if err != nil {
		return err
	}

	return os.RemoveAll(p.folder)
}

// EntityIds of the source vertices in the graph.
func (p *PebbleUnipartiteGraphStore) EntityIds() (*set.Set[string], error) {

	entityIds := set.NewSet[string]()

	iter := p.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		id := pebbleKeyToEntityId(iter.Key())
		entityIds.Add(id)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return entityIds, nil
}

// EdgeExists returns true if the two entities are connected.
func (p *PebbleUnipartiteGraphStore) EdgeExists(entity1 string, entity2 string) (bool, error) {

	// Preconditions
	err := ValidateEntityId(entity1)
	if err != nil {
		return false, ErrEntityIdIsBlank
	}

	err = ValidateEntityId(entity2)
	if err != nil {
		return false, ErrEntityIdIsBlank
	}

	dsts, found, err := p.dstEntityIds(entity1)
	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	return dsts.Has(entity2), nil
}

// EntityIdsAdjacentTo a given entity.
func (p *PebbleUnipartiteGraphStore) EntityIdsAdjacentTo(entityId string) (*set.Set[string], error) {

	// Preconditions
	err := ValidateEntityId(entityId)
	if err != nil {
		return nil, ErrEntityIdIsBlank
	}

	dsts, found, err := p.dstEntityIds(entityId)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("entity ID not found: %v", entityId)
	}

	return dsts, nil
}

func (p *PebbleUnipartiteGraphStore) NumberEntities() (int, error) {

	numEntities := 0

	iter := p.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		numEntities += 1
	}

	if err := iter.Close(); err != nil {
		return numEntities, err
	}

	return numEntities, nil
}
