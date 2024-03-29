package graphstore

import (
	"fmt"
	"sync"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// InMemoryUnipartiteGraphStore is a thread-safe in-memory unipartite graph store.
type InMemoryUnipartiteGraphStore struct {
	mu       sync.RWMutex
	vertices map[string]*set.Set[string]
}

// Instantiate an in-memory unipartite graph store.
func NewInMemoryUnipartiteGraphStore() *InMemoryUnipartiteGraphStore {
	return &InMemoryUnipartiteGraphStore{
		vertices: map[string]*set.Set[string]{},
	}
}

// AddEntity to the in-memory unipartite graph.
func (graph *InMemoryUnipartiteGraphStore) AddEntity(entity string) error {

	// Preconditions
	err := ValidateEntityId(entity)
	if err != nil {
		return err
	}

	// If the entity hasn't been seen before, add it to the graph
	if found, _ := graph.HasEntity(entity); !found {
		graph.mu.Lock()
		graph.vertices[entity] = set.NewSet[string]()
		graph.mu.Unlock()
	}

	return nil
}

// AddDirected edge between two vertices.
func (graph *InMemoryUnipartiteGraphStore) AddDirected(src string, dst string) error {

	// Preconditions
	err := ValidateEntityId(src)
	if err != nil {
		return err
	}

	err = ValidateEntityId(dst)
	if err != nil {
		return err
	}

	if src == dst {
		return fmt.Errorf("source and destination IDs are identical (%v)", src)
	}

	// If the source hasn't been seen before, add it to the graph
	graph.mu.Lock()
	_, found := graph.vertices[src]
	if !found {
		graph.vertices[src] = set.NewSet[string]()
	}
	x := graph.vertices[src]
	x.Add(dst)
	graph.mu.Unlock()

	return nil
}

// AddUndirected edge between two entities.
func (graph *InMemoryUnipartiteGraphStore) AddUndirected(v1 string, v2 string) error {

	// Preconditions
	// Validation of v1 and v2 is performed in the call to AddDirected

	// Add the connection v1 ---> v2
	err := graph.AddDirected(v1, v2)
	if err != nil {
		return err
	}

	// Add the connection v1 <--- v2
	return graph.AddDirected(v2, v1)
}

// Clear the in-memory unipartite graph store.
func (graph *InMemoryUnipartiteGraphStore) Clear() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Clearing the in-memory unipartite graph store")

	graph.mu.Lock()
	graph.vertices = map[string]*set.Set[string]{}
	graph.mu.Unlock()

	return nil
}

func (graph *InMemoryUnipartiteGraphStore) Close() error {
	return nil
}

func (graph *InMemoryUnipartiteGraphStore) Finalise() error {
	return nil
}

// Destroy the in-memory unipartite graph.
func (graph *InMemoryUnipartiteGraphStore) Destroy() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Destroying the in-memory unipartite graph store")

	return graph.Clear()
}

// EdgeExists between entity 1 and entity 2?
func (graph *InMemoryUnipartiteGraphStore) EdgeExists(entity1 string, entity2 string) (bool, error) {

	// Preconditions
	err := ValidateEntityId(entity1)
	if err != nil {
		return false, err
	}

	err = ValidateEntityId(entity2)
	if err != nil {
		return false, err
	}

	// Check if entity1 and entity2 both exist
	found1, _ := graph.HasEntity(entity1)
	found2, _ := graph.HasEntity(entity2)

	if !found1 || !found2 {
		return false, nil
	}

	// Both entities exist, so is there an edge between them?
	graph.mu.RLock()
	edgeExists := graph.vertices[entity1].Has(entity2)
	graph.mu.RUnlock()

	return edgeExists, nil
}

// EntityIdsAdjacentTo a given vertex with a given entity ID.
func (graph *InMemoryUnipartiteGraphStore) EntityIdsAdjacentTo(entityId string) (*set.Set[string], error) {

	// Preconditions
	err := ValidateEntityId(entityId)
	if err != nil {
		return nil, err
	}

	graph.mu.RLock()
	entityIds, found := graph.vertices[entityId]
	graph.mu.RUnlock()

	if !found {
		return nil, fmt.Errorf("entity ID not found: %v", entityId)
	}

	return entityIds, nil
}

// EntityIds held within the graph.
func (graph *InMemoryUnipartiteGraphStore) EntityIds() (*set.Set[string], error) {

	ids := set.NewSet[string]()

	graph.mu.RLock()
	for id := range graph.vertices {
		ids.Add(id)
	}
	graph.mu.RUnlock()

	return ids, nil
}

// HasEntity returns whether the store contains the entity.
func (graph *InMemoryUnipartiteGraphStore) HasEntity(id string) (bool, error) {

	// Preconditions
	err := ValidateEntityId(id)
	if err != nil {
		return false, err
	}

	graph.mu.RLock()
	_, found := graph.vertices[id]
	graph.mu.RUnlock()

	return found, nil
}

// NumberEntities in the store.
func (graph *InMemoryUnipartiteGraphStore) NumberEntities() (int, error) {

	// The graph is always used in undirected mode, so it's valid to just
	// count the number of source entities
	graph.mu.RLock()
	n := len(graph.vertices)
	graph.mu.RUnlock()

	return n, nil
}
