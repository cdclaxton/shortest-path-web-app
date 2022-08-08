package graphstore

import "github.com/cdclaxton/shortest-path-web-app/set"

type UnipartiteGraphStore interface {
	AddEntity(string)                                     // Add an entity
	AddDirected(string, string) error                     // Add a directed edge between two entities
	AddUndirected(string, string) error                   // Add an undirected edge between two entities
	Clear() error                                         // Clear down the graph
	EntityIds() *set.Set[string]                          // All entity IDs in the graph
	EntityIdsAdjacentTo(string) (*set.Set[string], error) // Entity IDs adjacent to a given entity ID
	Equal(UnipartiteGraphStore) bool                      // Are two unipartite graph stores identical?
}
