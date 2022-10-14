package graphstore

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

type InMemoryUnipartiteGraphStore struct {
	vertices map[string]*set.Set[string]
}

func NewInMemoryUnipartiteGraphStore() *InMemoryUnipartiteGraphStore {
	return &InMemoryUnipartiteGraphStore{
		vertices: map[string]*set.Set[string]{},
	}
}

// AddEntity to the unipartite graph.
func (graph *InMemoryUnipartiteGraphStore) AddEntity(entity string) error {

	// If the entity hasn't been seen before, add it to the graph
	if found, _ := graph.HasEntity(entity); !found {
		graph.vertices[entity] = set.NewSet[string]()
	}

	return nil
}

// AddDirected edge between two vertices.
func (graph *InMemoryUnipartiteGraphStore) AddDirected(src string, dst string) error {

	// Preconditions
	if src == dst {
		return fmt.Errorf("Source and destination IDs are identical (%v)", src)
	}

	// If the source hasn't been seen before, add it to the graph
	graph.AddEntity(src)

	// Add the connection from the source to the destination
	x := graph.vertices[src]
	x.Add(dst)

	return nil
}

// AddUndirected edge between two entities.
func (graph *InMemoryUnipartiteGraphStore) AddUndirected(v1 string, v2 string) error {

	// Add the connection v1 ---> v2
	err := graph.AddDirected(v1, v2)
	if err != nil {
		return err
	}

	// Add the connection v1 <--- v2
	return graph.AddDirected(v2, v1)
}

// Clear the unipartite graph store.
func (graph *InMemoryUnipartiteGraphStore) Clear() error {
	graph.vertices = map[string]*set.Set[string]{}
	return nil
}

// Destroy the unipartite graph.
func (graph *InMemoryUnipartiteGraphStore) Destroy() error {
	return graph.Clear()
}

// EdgeExists between entity 1 and entity 2?
func (graph *InMemoryUnipartiteGraphStore) EdgeExists(entity1 string, entity2 string) (bool, error) {

	found1, _ := graph.HasEntity(entity1)
	found2, _ := graph.HasEntity(entity2)

	if !found1 || !found2 {
		return false, nil
	}

	return graph.vertices[entity1].Has(entity2), nil
}

// EntityIdsAdjacentTo a given vertex with a given entity ID.
func (graph *InMemoryUnipartiteGraphStore) EntityIdsAdjacentTo(entityId string) (*set.Set[string], error) {

	entityIds, found := graph.vertices[entityId]

	if !found {
		return nil, fmt.Errorf("Entity ID not found: %v", entityId)
	}

	return entityIds, nil
}

// EntityIds held within the graph.
func (graph *InMemoryUnipartiteGraphStore) EntityIds() (*set.Set[string], error) {

	ids := set.NewSet[string]()

	for id := range graph.vertices {
		ids.Add(id)
	}

	return ids, nil
}

// HasEntity returns whether the store contains the entity.
func (graph *InMemoryUnipartiteGraphStore) HasEntity(id string) (bool, error) {
	_, found := graph.vertices[id]
	return found, nil
}
