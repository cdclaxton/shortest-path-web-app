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
func (graph *InMemoryUnipartiteGraphStore) AddEntity(entity string) {
	// If the entity hasn't been seen before, add it to the graph
	if _, present := graph.vertices[entity]; !present {
		graph.vertices[entity] = set.NewSet[string]()
	}
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

// EdgeExists between entity 1 and entity 2?
func (graph *InMemoryUnipartiteGraphStore) EdgeExists(entity1 string, entity2 string) bool {

	if !graph.HasEntity(entity1) {
		return false
	}

	if !graph.HasEntity(entity2) {
		return false
	}

	return graph.vertices[entity1].Has(entity2)
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
func (graph *InMemoryUnipartiteGraphStore) EntityIds() *set.Set[string] {

	ids := set.NewSet[string]()

	for id := range graph.vertices {
		ids.Add(id)
	}

	return ids
}

// Are two unipartite graph stores identical? This is for testing purposes.
func (graph *InMemoryUnipartiteGraphStore) Equal(g2 UnipartiteGraphStore) bool {

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

// HasEntity returns whether the store contains the entity.
func (graph *InMemoryUnipartiteGraphStore) HasEntity(id string) bool {
	_, found := graph.vertices[id]
	return found
}

// BuildFromEdgeList builds the graph from an undirected edge list.
func (graph *InMemoryUnipartiteGraphStore) BuildFromEdgeList(edges []Edge) error {
	for _, edge := range edges {
		err := graph.AddUndirected(edge.V1, edge.V2)
		if err != nil {
			return err
		}
	}

	return nil
}
