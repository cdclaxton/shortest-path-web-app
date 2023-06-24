// This component provides spidering from a set of seed entities.
//
// The minimum number of steps is zero. In this case, the result returned will allow the connections
// between the seed entities to be obtained.
//
// It is an error to run spidering with no seed entities.

package spider

import (
	"errors"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// Component name to use in logging
const componentName = "spider"

// Errors
var (
	ErrUnipartiteIsNil    = errors.New("unipartite graph is nil")
	ErrInvalidNumberSteps = errors.New("invalid number of steps")
	ErrNoSeedEntities     = errors.New("no seed entities")
)

// SpiderResults holds the sub-graph generated by spidering out from the seed entities.
type SpiderResults struct {
	NumberSteps          int
	Subgraph             *graphstore.InMemoryUnipartiteGraphStore // Sub-graph from spidering from seeds
	SeedEntities         *set.Set[string]                         // All entities set as seeds (even if they don't exist)
	SeedEntitiesNotFound *set.Set[string]                         // Entity IDs not found in unipartite graph
}

// NewSpiderResults returns a new SpiderResults struct with an empty sub-graph.
func NewSpiderResults(numberSteps int, seedEntities *set.Set[string]) *SpiderResults {

	// Instantiate a struct to hold the results
	results := SpiderResults{
		NumberSteps:          numberSteps,
		Subgraph:             graphstore.NewInMemoryUnipartiteGraphStore(),
		SeedEntities:         seedEntities,
		SeedEntitiesNotFound: set.NewSet[string](),
	}

	return &results
}

// HasAtLeastOneConnection returns true if at least two entities are connected.
func (s *SpiderResults) HasAtLeastOneConnection() (bool, error) {
	ids, err := s.Subgraph.EntityIds()
	if err != nil {
		return false, err
	}

	for _, id := range ids.ToSlice() {
		adj, err := s.Subgraph.EntityIdsAdjacentTo(id)
		if err != nil {
			return false, err
		}

		if adj.Len() > 0 {
			return true, nil
		}
	}

	return false, nil
}

// Equal returns true if the SpiderResults are equal.
func (s *SpiderResults) Equal(s2 *SpiderResults) (bool, error) {

	graphsEqual, _, err := graphstore.UnipartiteGraphStoresEqual(s.Subgraph, s2.Subgraph)

	if err != nil {
		return false, err
	}

	return s.NumberSteps == s2.NumberSteps &&
		s.SeedEntities.Equal(s2.SeedEntities) &&
		s.SeedEntitiesNotFound.Equal(s2.SeedEntitiesNotFound) &&
		graphsEqual, nil
}

// Spider is a component that generates a sub-graph by walking connections from a given set of
// 'seed' entities.
type Spider struct {
	unipartiteGraph graphstore.UnipartiteGraphStore
}

// NewSpider given a unipartite graph.
func NewSpider(graph graphstore.UnipartiteGraphStore) (*Spider, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Creating a new spider engine")

	if graph == nil {
		return nil, ErrUnipartiteIsNil
	}

	return &Spider{
		unipartiteGraph: graph,
	}, nil
}

// addSeedsAndConnections adds the seed entity to the unipartite sub-graph and the connections
// between seeds where present in the full graph.
func (s *Spider) addSeedsAndConnections(results *SpiderResults) error {

	for _, seedEntityId := range results.SeedEntities.ToSlice() {

		// If the seed entity ID cannot be found in the unipartite graph store, record it in the
		// results
		found, err := s.unipartiteGraph.HasEntity(seedEntityId)
		if err != nil {
			return err
		}
		if !found {
			results.SeedEntitiesNotFound.Add(seedEntityId)
			continue
		}

		// Add the seed entity to the unipartite sub-graph
		if err := results.Subgraph.AddEntity(seedEntityId); err != nil {
			return err
		}
	}

	// Add the connections between seed entities
	seedEntitiesInFullGraph, err := results.Subgraph.EntityIds()
	if err != nil {
		return err
	}

	for _, seedEntityId := range seedEntitiesInFullGraph.ToSlice() {
		adjacentEntityIds, err := s.unipartiteGraph.EntityIdsAdjacentTo(seedEntityId)
		if err != nil {
			return err
		}

		overlap := adjacentEntityIds.Intersection(seedEntitiesInFullGraph)
		if overlap.Len() > 0 {
			for _, adjEntityId := range overlap.ToSlice() {
				results.Subgraph.AddUndirected(seedEntityId, adjEntityId)
			}
		}
	}

	return nil
}

// spiderOutOneStep from all of the entities in the sub-graph in the results.
func (s *Spider) spiderOutOneStep(results *SpiderResults) error {

	entityIdInSubGraph, err := results.Subgraph.EntityIds()
	if err != nil {
		return err
	}

	for _, entityId := range entityIdInSubGraph.ToSlice() {

		// Find the adjacent entity IDs
		adjEntityIds, err := s.unipartiteGraph.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return err
		}

		// Add connections from the entity to all of its adjacent entities in the sub-graph
		for _, adjEntityId := range adjEntityIds.ToSlice() {
			results.Subgraph.AddUndirected(entityId, adjEntityId)
		}
	}

	return nil
}

// Execute spidering from a set of seed entities.
func (s *Spider) Execute(numberSteps int, seedEntities *set.Set[string]) (*SpiderResults, error) {

	// Check the number of steps is valid
	if numberSteps < 0 {
		return nil, ErrInvalidNumberSteps
	}

	// Check the seed entities
	if seedEntities.Len() == 0 {
		return nil, ErrNoSeedEntities
	}

	// Initialise the results
	results := NewSpiderResults(numberSteps, seedEntities)

	// Add connections between seed entities
	if err := s.addSeedsAndConnections(results); err != nil {
		return nil, err
	}

	// Add the directly connected entities
	for i := 1; i <= numberSteps; i++ {
		if err := s.spiderOutOneStep(results); err != nil {
			return nil, err
		}
	}

	return results, nil
}
