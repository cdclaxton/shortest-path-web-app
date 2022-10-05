package graphstore

import (
	"fmt"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

type Edge struct {
	V1 string
	V2 string
}

type UnipartiteGraphStore interface {
	AddEntity(string) error                               // Add an entity
	AddDirected(string, string) error                     // Add a directed edge between two entities
	AddUndirected(string, string) error                   // Add an undirected edge between two entities
	Clear() error                                         // Clear down the graph
	EdgeExists(string, string) (bool, error)              // Are the two entities connected?
	EntityIds() (*set.Set[string], error)                 // All entity IDs in the graph
	EntityIdsAdjacentTo(string) (*set.Set[string], error) // Entity IDs adjacent to a given entity ID
	HasEntity(string) (bool, error)                       // Does the store contain the entity?
}

// BuildFromEdgeList builds the graph from an undirected edge list.
func BuildFromEdgeList(graph UnipartiteGraphStore, edges []Edge) error {

	// Preconditions
	if graph == nil {
		return fmt.Errorf("Unipartite graph store is nil")
	}

	if edges == nil {
		return fmt.Errorf("Edges is nil")
	}

	// Walk through each edge and add it to the graph
	for _, edge := range edges {
		err := graph.AddUndirected(edge.V1, edge.V2)
		if err != nil {
			return err
		}
	}

	return nil
}

// UnipartiteGraphStoresEqual returns true if two unipartite graph stores are identical.
func UnipartiteGraphStoresEqual(g1 UnipartiteGraphStore, g2 UnipartiteGraphStore) (bool, error) {

	// Get lists of entity IDs
	entityIds1, err := g1.EntityIds()
	if err != nil {
		return false, err
	}

	entityIds2, err := g2.EntityIds()
	if err != nil {
		return false, err
	}

	// Check whether the entity IDs are identical
	if !entityIds1.Equal(entityIds2) {
		return false, nil
	}

	// Check the connections
	for _, entityId := range entityIds1.ToSlice() {

		// Connections from the entity
		conns1, err := g1.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return false, nil
		}

		conns2, err := g2.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return false, nil
		}

		if !conns1.Equal(conns2) {
			return false, nil
		}
	}

	return true, nil
}
