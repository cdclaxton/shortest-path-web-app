package graphstore

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

type InMemoryUnipartiteGraphStorage struct {
	vertices map[string]set.Set[string]
}

func NewInMemoryUnipartiteGraphStorage() *InMemoryUnipartiteGraphStorage {
	return &InMemoryUnipartiteGraphStorage{
		vertices: map[string]set.Set[string]{},
	}
}

// AddDirected edge between two vertices.
func (graph *InMemoryUnipartiteGraphStorage) AddDirected(src string, dst string) error {

	// Preconditions
	if src == dst {
		return fmt.Errorf("Source and destination IDs are identical (%v)", src)
	}

	// If the source hasn't been seen before, add it to the graph
	if _, present := graph.vertices[src]; !present {
		graph.vertices[src] = set.NewSet[string]()
	}

	// Add the connection from the source to the destination
	x := graph.vertices[src]
	x.Add(dst)

	return nil
}

// AddUnidirected edge between two entities.
func (graph *InMemoryUnipartiteGraphStorage) AddUndirected(v1 string, v2 string) error {

	// Add the connection v1 ---> v2
	err := graph.AddDirected(v1, v2)
	if err != nil {
		return err
	}

	// Add the connection v2 ---> v1
	return graph.AddDirected(v1, v2)
}

func (graph *InMemoryUnipartiteGraphStorage) Clear() error {
	graph.vertices = map[string]set.Set[string]{}
	return nil
}

// EntityIdsAdjacentTo a given vertex with a given entity ID.
func (graph *InMemoryUnipartiteGraphStorage) EntityIdsAdjacentTo(entityId string) (*set.Set[string], error) {

	entityIds, found := graph.vertices[entityId]

	if !found {
		return nil, fmt.Errorf("Entity ID not found: %v", entityId)
	}

	return &entityIds, nil
}

// EntityIds held within the graph.
func (graph *InMemoryUnipartiteGraphStorage) EntityIds() *set.Set[string] {

	ids := set.NewSet[string]()

	for id := range graph.vertices {
		ids.Add(id)
	}

	return &ids
}

// Are two unipartite graph stores identical? This is for testing purposes.
func (graph *InMemoryUnipartiteGraphStorage) Equal(g2 UnipartiteGraphStorage) bool {

	// Get lists of entity IDs
	entityIds1 := graph.EntityIds()
	entityIds2 := g2.EntityIds()

	// Check the entity IDs are identical
	if !entityIds1.Equal(entityIds2) {
		return false
	}

	// Check the connections
	for _, entityId := range entityIds1.ToSlice() {
		conns1, _ := graph.EntityIdsAdjacentTo(entityId)
		conns2, _ := g2.EntityIdsAdjacentTo(entityId)

		if !conns1.Equal(conns2) {
			return false
		}
	}

	return true
}
