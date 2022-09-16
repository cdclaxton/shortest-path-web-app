package graphstore

import "github.com/cdclaxton/shortest-path-web-app/set"

type Edge struct {
	V1 string
	V2 string
}

type UnipartiteGraphStore interface {
	AddEntity(string)                                     // Add an entity
	AddDirected(string, string) error                     // Add a directed edge between two entities
	AddUndirected(string, string) error                   // Add an undirected edge between two entities
	BuildFromEdgeList([]Edge) error                       // Build the graph from an undirected edge list
	Clear() error                                         // Clear down the graph
	EdgeExists(string, string) bool                       // Are the two entities connected?
	EntityIds() *set.Set[string]                          // All entity IDs in the graph
	EntityIdsAdjacentTo(string) (*set.Set[string], error) // Entity IDs adjacent to a given entity ID
	Equal(UnipartiteGraphStore) bool                      // Are two unipartite graph stores identical?
	HasEntity(string) bool                                // Does the store contain the entity?
}
