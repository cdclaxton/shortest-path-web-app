// A Pebble unipartite graph store is a database-backed component for holding a graph structure
// of a single type of node. Pebble DB is a key-value store and it provides the persistence.
//
// A new Pebble store is created using the NewPebbleUnipartiteGraphStore() function. Note that
// the function opens the database connection ready for reading. Once the database is no longer
// needed (e.g. because the program using the database is closing down), the Close() method must be
// called.
//
// To avoid a significant number of reads and writes during the load stage, the key-value pair
// design is:
//
// e#<src entity ID>#<dst entity ID>
//
// The hash symbol was chosen because it is not a valid character in an entity ID. To store an
// entity without a connection:
//
// n#<entity ID>

package graphstore

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cockroachdb/pebble"
)

const (
	nodePrefix       = "n"
	edgePrefix       = "e"
	separator        = "#"
	separatorPlusOne = "$"
)

var (
	ErrEmptyEntityId                    = errors.New("empty entity ID")
	ErrEntityIdContainsIllegalCharacter = errors.New("entity ID contains illegal character")
	ErrMalformedKey                     = errors.New("malformed unipartite key")
	ErrUnexpectedEntityInKey            = errors.New("unexpected entity ID in key")
	ErrSelfLoop                         = errors.New("self loop")
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

// validateEntityId validates the entity ID prior to storage.
func validateEntityId(id string) error {

	if len(id) == 0 {
		return ErrEmptyEntityId
	}

	if strings.Contains(id, separator) {
		return ErrEntityIdContainsIllegalCharacter
	}

	return nil
}

// edgeToPebbleKey returns the Pebble key for a directed edge between two entities.
func edgeToPebbleKey(src string, dst string) ([]byte, error) {

	if err := validateEntityId(src); err != nil {
		return nil, err
	}

	if err := validateEntityId(dst); err != nil {
		return nil, err
	}

	if src == dst {
		return nil, ErrSelfLoop
	}

	return []byte(edgePrefix + separator + src + separator + dst), nil
}

// pebbleKeyToEdge returns the source and destination nodes for a key representing an edge.
func pebbleKeyToEdge(key []byte) (string, string, error) {

	parts := strings.Split(string(key), separator)

	if len(parts) != 3 {
		return "", "", fmt.Errorf("%w: %v", ErrMalformedKey, string(key))
	}

	if parts[0] != edgePrefix {
		return "", "", fmt.Errorf("%w: %v is not an edge", ErrMalformedKey, string(key))
	}

	src := parts[1]
	dst := parts[2]

	if validateEntityId(src) != nil || validateEntityId(dst) != nil {
		return "", "", fmt.Errorf("%w: %v is not a valid edge", ErrMalformedKey, string(key))
	}

	return src, dst, nil
}

// nodeToPebbleKey returns the Pebble key for a node.
func nodeToPebbleKey(node string) ([]byte, error) {

	if err := validateEntityId(node); err != nil {
		return nil, err
	}

	return []byte(nodePrefix + separator + node), nil
}

// pebbleKeyToNode returns the node for a Pebble key.
func pebbleKeyToNode(key []byte) (string, error) {

	parts := strings.Split(string(key), separator)

	if len(parts) != 2 {
		return "", fmt.Errorf("%w: %v", ErrMalformedKey, string(key))
	}

	if parts[0] != nodePrefix {
		return "", fmt.Errorf("%w: %v is not a node", ErrMalformedKey, string(key))
	}

	node := parts[1]
	if err := validateEntityId(node); err != nil {
		return "", err
	}

	return node, nil
}

// AddEntity to the unipartite graph store.
func (p *PebbleUnipartiteGraphStore) AddEntity(id string) error {

	key, err := nodeToPebbleKey(id)
	if err != nil {
		return err
	}

	return p.db.Set(key, nil, pebble.NoSync)
}

// AddDirected edge between the source (src) and destination (dst) vertices.
func (p *PebbleUnipartiteGraphStore) AddDirected(src string, dst string) error {

	key, err := edgeToPebbleKey(src, dst)
	if err != nil {
		return err
	}

	return p.db.Set(key, nil, pebble.NoSync)
}

// AddUndirected edge between two entities.
func (p *PebbleUnipartiteGraphStore) AddUndirected(src string, dst string) error {

	// Add the src --> dst connection
	err := p.AddDirected(src, dst)
	if err != nil {
		return err
	}

	// Add the src <-- dst connection
	return p.AddDirected(dst, src)
}

// EdgeExists returns true if the two entities are connected.
func (p *PebbleUnipartiteGraphStore) EdgeExists(src string, dst string) (bool, error) {

	key, err := edgeToPebbleKey(src, dst)
	if err != nil {
		return false, err
	}

	_, closer, err := p.db.Get(key)

	if err == pebble.ErrNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if err2 := closer.Close(); err2 != nil {
		return false, err2
	}

	return true, nil
}

// entityIdsOfNodes returns the entity IDs of nodes.
func (p *PebbleUnipartiteGraphStore) entityIdsOfNodes() (*set.Set[string], error) {

	entityIds := set.NewSet[string]()

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(nodePrefix + separator),
		UpperBound: []byte(nodePrefix + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	var errDuringIteration error
	for iter.First(); iter.Valid() && errDuringIteration == nil; iter.Next() {
		var src string
		src, errDuringIteration = pebbleKeyToNode(iter.Key())

		if errDuringIteration == nil {
			entityIds.Add(src)
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

// entityIdsOfEdges returns the entity IDs of entities with edges.
func (p *PebbleUnipartiteGraphStore) entityIdsOfEdges() (*set.Set[string], error) {

	entityIds := set.NewSet[string]()

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(edgePrefix + separator),
		UpperBound: []byte(edgePrefix + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	var errDuringIteration error
	var src string
	for iter.First(); iter.Valid() && errDuringIteration == nil; iter.Next() {
		src, _, errDuringIteration = pebbleKeyToEdge(iter.Key())

		if errDuringIteration == nil {
			entityIds.Add(src)
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

// EntityIds of the source vertices in the graph.
func (p *PebbleUnipartiteGraphStore) EntityIds() (*set.Set[string], error) {

	entityIds, err := p.entityIdsOfNodes()
	if err != nil {
		return nil, err
	}

	entityIds2, err := p.entityIdsOfEdges()
	if err != nil {
		return nil, err
	}

	return entityIds.Union(entityIds2), nil
}

// EntityIdsAdjacentTo a given entity.
func (p *PebbleUnipartiteGraphStore) EntityIdsAdjacentTo(id string) (*set.Set[string], error) {

	adjacentIds := set.NewSet[string]()

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(edgePrefix + separator + id + separator),
		UpperBound: []byte(edgePrefix + separator + id + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	var errDuringIteration error
	for iter.First(); iter.Valid() && errDuringIteration == nil; iter.Next() {
		var src, dst string
		src, dst, errDuringIteration = pebbleKeyToEdge(iter.Key())

		if errDuringIteration == nil {
			if src != id {
				errDuringIteration = ErrUnexpectedEntityInKey
			} else {
				adjacentIds.Add(dst)
			}
		}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	if errDuringIteration != nil {
		return nil, errDuringIteration
	}

	if adjacentIds.Len() == 0 {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, id)
	}

	return adjacentIds, nil
}

func (p *PebbleUnipartiteGraphStore) hasNode(id string) (bool, error) {

	key, err := nodeToPebbleKey(id)
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

	defer closer.Close()

	return true, nil
}

func (p *PebbleUnipartiteGraphStore) hasEdgeWithSource(id string) (bool, error) {

	iterOptions := &pebble.IterOptions{
		LowerBound: []byte(edgePrefix + separator + id + separator),
		UpperBound: []byte(edgePrefix + separator + id + separatorPlusOne),
	}

	iter := p.db.NewIter(iterOptions)
	found := iter.First()

	if err := iter.Close(); err != nil {
		return false, err
	}

	return found, nil
}

// HasEntity returns true if the entity ID is held within the backend.
func (p *PebbleUnipartiteGraphStore) HasEntity(id string) (bool, error) {

	// Check whether the entity exists on its own
	found, err := p.hasNode(id)
	if err != nil {
		return false, err
	}

	if found {
		return true, nil
	}

	// Check whether the entity exists as the source of an edge
	return p.hasEdgeWithSource(id)
}

// NumberEntities in the unipartite graph.
func (p *PebbleUnipartiteGraphStore) NumberEntities() (int, error) {

	entityIds, err := p.EntityIds()
	if err != nil {
		return 0, err
	}

	return entityIds.Len(), nil
}
