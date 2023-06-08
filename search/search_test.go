package search

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {

	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	testCases := []struct {
		entityIds       []string
		expectedResults map[string]EntitySearchResult
	}{
		{
			// One entity (in both graph stores)
			entityIds: []string{"e-1"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
			},
		},
		{
			// One entity (in neither graph store)
			entityIds: []string{"e-5"},
			expectedResults: map[string]EntitySearchResult{
				"e-5": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
		{
			// Two entities
			entityIds: []string{"e-1", "e-2"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-2": {
					InUnipartite: true,
					InBipartite:  true,
				},
			},
		},
		{
			// Three entities
			entityIds: []string{"e-1", "e-2", "e-5"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-2": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-5": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
		{
			// Similar entity IDs
			entityIds: []string{"e-1", "e-1 ", "e-11", "e-10", "e-"},
			expectedResults: map[string]EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  true,
				},
				"e-1 ": {
					InUnipartite: false,
					InBipartite:  false,
				},
				"e-11": {
					InUnipartite: false,
					InBipartite:  false,
				},
				"e-10": {
					InUnipartite: false,
					InBipartite:  false,
				},
				"e-": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		// Run the search tests
		for _, testCase := range testCases {
			actual, err := engine.Search(testCase.entityIds)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedResults, actual)
		}

		// Destroy the graph databases
		graphBuilder.Destroy()
	}
}

func TestConvertAndSortAttributes(t *testing.T) {

	testCases := []struct {
		attributes map[string]string
		expected   []Attribute
	}{
		{
			// Empty attributes
			attributes: map[string]string{},
			expected:   []Attribute{},
		},
		{
			// One attribute
			attributes: map[string]string{
				"Name": "Bob",
			},
			expected: []Attribute{
				{
					Key:   "Name",
					Value: "Bob",
				},
			},
		},
		{
			// Two attributes
			attributes: map[string]string{
				"Forename": "Bob",
				"Surname":  "Smith",
			},
			expected: []Attribute{
				{
					Key:   "Forename",
					Value: "Bob",
				},
				{
					Key:   "Surname",
					Value: "Smith",
				},
			},
		},
		{
			// Two attributes
			attributes: map[string]string{
				"Forename": "Bob",
				"Surname":  "Smith",
				"Age":      "21",
			},
			expected: []Attribute{
				{
					Key:   "Age",
					Value: "21",
				},
				{
					Key:   "Forename",
					Value: "Bob",
				},
				{
					Key:   "Surname",
					Value: "Smith",
				},
			},
		},
	}

	for _, testCase := range testCases {
		actual := convertAndSortAttributes(testCase.attributes)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestExtractDocuments(t *testing.T) {

	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	documentIds := set.NewPopulatedSet("d-1", "d-2", "d-10")
	expected := []BipartiteDocument{
		{
			DocumentId:   "d-1",
			FoundInStore: true,
			Type:         "Doc-type-A",
			Attributes: []Attribute{
				{
					Key:   "Date",
					Value: "06/08/2022",
				},
				{
					Key:   "Title",
					Value: "Summary 1",
				},
			},
		},
		{
			DocumentId:   "d-10",
			FoundInStore: false,
			Type:         "",
			Attributes:   []Attribute{},
		},
		{
			DocumentId:   "d-2",
			FoundInStore: true,
			Type:         "Doc-type-A",
			Attributes: []Attribute{
				{
					Key:   "Date",
					Value: "07/08/2022",
				},
				{
					Key:   "Title",
					Value: "Summary 2",
				},
			},
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		actual, err := engine.extractDocuments(documentIds)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)

		// Destroy the graph databases
		graphBuilder.Destroy()
	}
}

func TestEntityToBipartiteDetails(t *testing.T) {
	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	entity, err := graphstore.NewEntity("e-10", "Person", map[string]string{
		"Name": "Bob Smith",
		"Age":  "21",
	})
	assert.NoError(t, err)

	entity.AddDocument("d-100")
	entity.AddDocument("d-101")
	entity.AddDocument("d-102") // This won't exist

	document1, err := graphstore.NewDocument("d-100", "Doc-C", map[string]string{
		"Date":  "19/01/2023",
		"Title": "Summary 100",
	})
	assert.NoError(t, err)
	document1.AddEntity("e-10")

	document2, err := graphstore.NewDocument("d-101", "Doc-C", map[string]string{
		"Date":  "20/01/2023",
		"Title": "Summary 101",
	})
	assert.NoError(t, err)
	document2.AddEntity("e-10")

	entityNotInStore, err := graphstore.NewEntity("e-11", "Person", map[string]string{
		"Name": "Angel Baker",
		"Age":  "22",
	})
	assert.NoError(t, err)

	// Details for entity e-100
	expected := BipartiteDetails{
		InBipartite: true,
		EntityType:  "Person",
		EntityAttributes: []Attribute{
			{
				Key:   "Age",
				Value: "21",
			},
			{
				Key:   "Name",
				Value: "Bob Smith",
			},
		},
		LinkedDocuments: []BipartiteDocument{
			{
				DocumentId:   "d-100",
				FoundInStore: true,
				Type:         "Doc-C",
				Attributes: []Attribute{
					{
						Key:   "Date",
						Value: "19/01/2023",
					},
					{
						Key:   "Title",
						Value: "Summary 100",
					},
				},
			},
			{
				DocumentId:   "d-101",
				FoundInStore: true,
				Type:         "Doc-C",
				Attributes: []Attribute{
					{
						Key:   "Date",
						Value: "20/01/2023",
					},
					{
						Key:   "Title",
						Value: "Summary 101",
					},
				},
			},
			{
				DocumentId:   "d-102",
				FoundInStore: false,
				Type:         "",
				Attributes:   []Attribute{},
			},
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		assert.NoError(t, graphBuilder.Bipartite.AddEntity(entity))
		assert.NoError(t, graphBuilder.Bipartite.AddDocument(document1))
		assert.NoError(t, graphBuilder.Bipartite.AddDocument(document2))

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		// Try to get an entity that doesn't exist
		_, err = engine.entityToBipartiteDetails(&entityNotInStore)
		assert.Error(t, err)

		actual, err := engine.entityToBipartiteDetails(&entity)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)

		// Destroy the graph databases
		graphBuilder.Destroy()
	}
}

func TestLinkedEntityPresence(t *testing.T) {

	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		// Test the case where the entity is not in the unipartite store or the bipartite store
		linkedEntities, err := engine.linkedEntityPresence("e-10")
		assert.NoError(t, err)
		assert.Equal(t, []EntityPresence{}, linkedEntities)

		// Entity is in the unipartite store, but is not connnected to any other entities
		engine.Unipartite.AddEntity("e-10")
		linkedEntities, err = engine.linkedEntityPresence("e-10")
		assert.NoError(t, err)
		assert.Equal(t, []EntityPresence{}, linkedEntities)

		// First hop is in unipartite store, but not in the bipartite store
		engine.Unipartite.AddUndirected("e-10", "e-11")
		linkedEntities, err = engine.linkedEntityPresence("e-10")
		assert.NoError(t, err)
		assert.Equal(t, []EntityPresence{
			{
				EntityId:     "e-11",
				InUnipartite: true,
				InBipartite:  false,
			},
		}, linkedEntities)

		// First hop is not in the unipartite store, but is in the bipartite store
		engine.Unipartite.AddEntity("e-101")
		engine.Unipartite.AddEntity("e-102")

		e100, err := graphstore.NewEntity("e-100", "Person", map[string]string{})
		assert.NoError(t, err)
		e101, err := graphstore.NewEntity("e-101", "Person", map[string]string{})
		assert.NoError(t, err)
		e102, err := graphstore.NewEntity("e-102", "Person", map[string]string{})
		assert.NoError(t, err)
		e103, err := graphstore.NewEntity("e-103", "Person", map[string]string{})
		assert.NoError(t, err)

		d100, err := graphstore.NewDocument("d-100", "Type-A", map[string]string{})
		assert.NoError(t, err)
		d101, err := graphstore.NewDocument("d-101", "Type-A", map[string]string{})
		assert.NoError(t, err)

		e100.AddDocument("d-100")
		e100.AddDocument("d-101")

		d100.AddEntity("e-100")
		d100.AddEntity("e-101")
		d100.AddEntity("e-102")

		d101.AddEntity("e-100")
		d101.AddEntity("e-103")

		assert.NoError(t, engine.Bipartite.AddEntity(e100))
		assert.NoError(t, engine.Bipartite.AddEntity(e101))
		assert.NoError(t, engine.Bipartite.AddEntity(e102))
		assert.NoError(t, engine.Bipartite.AddEntity(e103))

		assert.NoError(t, engine.Bipartite.AddDocument(d100))
		assert.NoError(t, engine.Bipartite.AddDocument(d101))

		actualEntityIds := engine.entityIdsFromBipartite("e-100")
		expectedEntityIds := set.NewPopulatedSet("e-101", "e-102", "e-103")
		assert.True(t, actualEntityIds.Equal(expectedEntityIds))

		linkedEntities, err = engine.linkedEntityPresence("e-100")
		assert.NoError(t, err)
		assert.Equal(t, []EntityPresence{
			{
				EntityId:     "e-101",
				InUnipartite: true,
				InBipartite:  true,
			},
			{
				EntityId:     "e-102",
				InUnipartite: true,
				InBipartite:  true,
			},
			{
				EntityId:     "e-103",
				InUnipartite: false,
				InBipartite:  true,
			},
		}, linkedEntities)

		// Destroy the graph databases
		graphBuilder.Destroy()
	}

}

func TestEntitySearch(t *testing.T) {

	backends := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	for _, backend := range backends {

		// Instantiate the graph builder
		graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(backend.configFilepath)
		assert.NoError(t, err)

		// Make the search engine
		engine, err := NewEntitySearch(graphBuilder.Bipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)

		searchResult := engine.GetEntity("e-1")
		expected := SearchEntity{
			EntityId: "e-1",
			Error: ErrorDetails{
				ErrorOccurred: false,
				ErrorMessage:  "",
			},
			BipartiteDetails: BipartiteDetails{
				InBipartite: true,
				EntityType:  "Person",
				EntityAttributes: []Attribute{
					{
						Key:   "Full Name",
						Value: "Bob Smith",
					},
				},
				LinkedDocuments: []BipartiteDocument{
					{
						DocumentId:   "d-1",
						FoundInStore: true,
						Type:         "Doc-type-A",
						Attributes: []Attribute{
							{
								Key:   "Date",
								Value: "06/08/2022",
							},
							{
								Key:   "Title",
								Value: "Summary 1",
							},
						},
					},
					{
						DocumentId:   "d-2",
						FoundInStore: true,
						Type:         "Doc-type-A",
						Attributes: []Attribute{
							{
								Key:   "Date",
								Value: "07/08/2022",
							},
							{
								Key:   "Title",
								Value: "Summary 2",
							},
						},
					},
					{
						DocumentId:   "d-3",
						FoundInStore: true,
						Type:         "Doc-type-B",
						Attributes: []Attribute{
							{
								Key:   "Date",
								Value: "09/08/2022",
							},
							{
								Key:   "Title",
								Value: "Summary 3",
							},
						},
					},
				},
			},
			InUnipartite: true,
			LinkedEntities: []EntityPresence{
				{
					EntityId:     "e-2",
					InBipartite:  true,
					InUnipartite: true,
				},
				{
					EntityId:     "e-3",
					InBipartite:  true,
					InUnipartite: true,
				},
			},
		}
		assert.Equal(t, expected, searchResult)

		// Destroy the graph databases
		graphBuilder.Destroy()
	}
}
