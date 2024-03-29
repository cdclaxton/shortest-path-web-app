package graphstore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/set"
)

// An Edge represents a link between two entities.
type Edge struct {
	V1 string
	V2 string
}

// edgeStringsToEdges converts a slice of edge definitions to a slice of edges. The edge definitions
// can take the form of "e1 - e2" where the separator is "-".
func edgeStringsToEdges(definitions []string, edgeSeparator string) ([]Edge, error) {

	edges := []Edge{}

	for _, d := range definitions {
		parts := strings.Split(d, edgeSeparator)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid edge definition: %s", d)
		}

		edges = append(edges, Edge{
			V1: strings.TrimSpace(parts[0]),
			V2: strings.TrimSpace(parts[1]),
		})
	}

	return edges, nil
}

// A UnipartiteGraphStore represents the store of a graph composed of a single type of vertex.
type UnipartiteGraphStore interface {
	AddEntity(string) error                               // Add an entity
	AddDirected(string, string) error                     // Add a directed edge between two entities
	AddUndirected(string, string) error                   // Add an undirected edge between two entities
	Clear() error                                         // Clear down the graph
	Close() error                                         // Close the graph
	Destroy() error                                       // Destroy the graph (and any backing files)
	EdgeExists(string, string) (bool, error)              // Are the two entities connected?
	EntityIds() (*set.Set[string], error)                 // All entity IDs in the graph
	EntityIdsAdjacentTo(string) (*set.Set[string], error) // Entity IDs adjacent to a given entity ID
	Finalise() error                                      // Run any tidy up actions
	HasEntity(string) (bool, error)                       // Does the store contain the entity?
	NumberEntities() (int, error)                         // Number of entities in the store
}

// BuildFromEdgeList builds the graph from an undirected edge list.
func BuildFromEdgeList(graph UnipartiteGraphStore, edges []Edge) error {

	// Preconditions
	if graph == nil {
		return errors.New("unipartite graph store is nil")
	}

	if edges == nil {
		return errors.New("edges is nil")
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
func UnipartiteGraphStoresEqual(g1 UnipartiteGraphStore, g2 UnipartiteGraphStore) (bool, string, error) {

	// Preconditions
	if g1 == nil {
		return false, "", errors.New("graph store g1 is nil")
	}

	if g2 == nil {
		return false, "", errors.New("graph store g2 is nil")
	}

	// Get lists of entity IDs
	entityIds1, err := g1.EntityIds()
	if err != nil {
		return false, "", err
	}

	entityIds2, err := g2.EntityIds()
	if err != nil {
		return false, "", err
	}

	// Check whether the entity IDs are identical
	if !entityIds1.Equal(entityIds2) {
		if entityIds1.Len() < 10 && entityIds2.Len() < 10 {
			fmt.Printf("entity IDs: %v vs %v\n", entityIds1.Values, entityIds2.Values)
		}

		return false, fmt.Sprintf("different entity IDs (%d vs %d)", entityIds1.Len(), entityIds2.Len()), nil
	}

	// Check the connections
	for _, entityId := range entityIds1.ToSlice() {

		// Connections from the entity
		conns1, err := g1.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return false, fmt.Sprintf("entity missing in g1: %s\n", entityId), nil
		}

		conns2, err := g2.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return false, fmt.Sprintf("entity missing in g2: %s\n", entityId), nil
		}

		if !conns1.Equal(conns2) {
			return false, fmt.Sprintf("different connections: %s (%d vs %d)\n", entityId, conns1.Len(), conns2.Len()), nil
		}
	}

	return true, "", nil
}

type UnipartiteStats struct {
	NumberOfEntities int // Number of entities in the unipartite store
}

func CalcUnipartiteStats(ug UnipartiteGraphStore) (UnipartiteStats, error) {

	numEntities, err := ug.NumberEntities()
	if err != nil {
		return UnipartiteStats{}, err
	}

	return UnipartiteStats{
		NumberOfEntities: numEntities,
	}, nil
}
