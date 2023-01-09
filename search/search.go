package search

import (
	"errors"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

const componentName = "search"

// EntitySearch finds entities in the bipartite and unipartite stores.
type EntitySearch struct {
	Bipartite  graphstore.BipartiteGraphStore
	Unipartite graphstore.UnipartiteGraphStore
}

// NewEntitySearch given the bipartite and unipartite stores.
func NewEntitySearch(bipartite graphstore.BipartiteGraphStore,
	unipartite graphstore.UnipartiteGraphStore) (*EntitySearch, error) {

	// Preconditions
	if bipartite == nil {
		return nil, errors.New("bipartite graph is nil")
	}

	if unipartite == nil {
		return nil, errors.New("unipartite graph is nil")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Making the entity search engine")

	return &EntitySearch{
		Bipartite:  bipartite,
		Unipartite: unipartite,
	}, nil
}

// EntitySearchResult for a single entity.
type EntitySearchResult struct {
	InUnipartite bool
	InBipartite  bool
}

// Search for entities given their IDs in the bipartite and unipartite stores.
func (es *EntitySearch) Search(entityIds []string) (map[string]EntitySearchResult, error) {

	searchResult := map[string]EntitySearchResult{}

	for _, entityId := range entityIds {

		// Try to find the entity in the bipartite graph
		entityInBipartite := false
		_, err := es.Bipartite.GetEntity(entityId)
		if err == graphstore.ErrEntityNotFound {
			entityInBipartite = false
		} else if err != nil {
			return nil, err
		} else {
			entityInBipartite = true
		}

		// Try to find the entity in the unipartite store
		entityInUnipartite, err := es.Unipartite.HasEntity(entityId)
		if err != nil {
			return nil, err
		}

		searchResult[entityId] = EntitySearchResult{
			InUnipartite: entityInUnipartite,
			InBipartite:  entityInBipartite,
		}
	}

	return searchResult, nil
}
