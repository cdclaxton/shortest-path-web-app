package i2chart

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cdclaxton/shortest-path-web-app/spider"
	"github.com/stretchr/testify/assert"
)

func TestReadSpiderI2ChartConfig(t *testing.T) {

	testCases := []struct {
		filepath      string
		expected      *SpiderI2ChartConfig
		errorExpected bool
	}{
		{
			// File doesn't exist
			filepath:      "./test-data/does-not-exist.json",
			expected:      nil,
			errorExpected: true,
		},
		{
			// Valid file
			filepath: "./test-data/spider-i2-config-1.json",
			expected: &SpiderI2ChartConfig{
				EntityConfig: map[string]SpiderEntityConfig{
					"Person": {
						Icon:  "Anonymous",
						Label: "<Full Name>",
					},
					"Address": {
						Icon:  "House",
						Label: "<Address>",
					},
				},
				UnknownEntityTypeIcon:  "UNKNOWN-1",
				UnknownEntityTypeLabel: "UNKNOWN-2",
				MissingAttribute:       "UNKNOWN-3",
			},
		},
	}

	for _, testCase := range testCases {
		actual, err := readSpiderI2ChartConfig(testCase.filepath)
		assert.Equal(t, testCase.expected, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestSortedEntityIds(t *testing.T) {
	testCases := []struct {
		ids      *set.Set[string]
		expected []string
	}{
		{
			ids:      set.NewSet[string](),
			expected: []string{},
		},
		{
			ids:      set.NewPopulatedSet("1"),
			expected: []string{"1"},
		},
		{
			ids:      set.NewPopulatedSet("1", "2"),
			expected: []string{"1", "2"},
		},
		{
			ids:      set.NewPopulatedSet("1", "2", "10"),
			expected: []string{"1", "10", "2"},
		},
	}

	for _, testCase := range testCases {
		actual := sortedEntityIds(testCase.ids)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestMakeEntityForI2(t *testing.T) {

	// Construct an in-memory bipartite graph store for the test
	bipartite := makeBipartiteStore(t)

	testCases := []struct {
		entityId      string
		entityIsSeed  bool
		config        SpiderI2ChartConfig
		expected      EntityForI2
		errorExpected bool
	}{
		{
			entityId:     "e-1",
			entityIsSeed: false,
			config: SpiderI2ChartConfig{
				EntityConfig: map[string]SpiderEntityConfig{
					"Person": {
						Icon:  "Anonymous",
						Label: "<Full Name>",
					},
				},
				UnknownEntityTypeIcon:  "UNKNOWN-ICON",
				UnknownEntityTypeLabel: "UNKNOWN-LABEL",
				MissingAttribute:       "UNKNOWN",
			},
			expected: EntityForI2{
				entityId:     "e-1",
				entityType:   "Person",
				entityIcon:   "Anonymous",
				entityLabel:  "Bob Smith",
				isSeedEntity: "FALSE",
			},
			errorExpected: false,
		},
		{
			entityId:     "e-1",
			entityIsSeed: true,
			config: SpiderI2ChartConfig{
				EntityConfig: map[string]SpiderEntityConfig{
					"Person": {
						Icon:  "Anonymous",
						Label: "<Full Name>",
					},
				},
				UnknownEntityTypeIcon:  "UNKNOWN-ICON",
				UnknownEntityTypeLabel: "UNKNOWN-LABEL",
				MissingAttribute:       "UNKNOWN",
			},
			expected: EntityForI2{
				entityId:     "e-1",
				entityType:   "Person",
				entityIcon:   "Anonymous",
				entityLabel:  "Bob Smith",
				isSeedEntity: "TRUE",
			},
			errorExpected: false,
		},
		{
			// Entity type specification not available
			entityId:     "e-1",
			entityIsSeed: false,
			config: SpiderI2ChartConfig{
				EntityConfig: map[string]SpiderEntityConfig{
					"Address": {
						Icon:  "House",
						Label: "<First line>",
					},
				},
				UnknownEntityTypeIcon:  "UNKNOWN-ICON",
				UnknownEntityTypeLabel: "UNKNOWN-LABEL",
				MissingAttribute:       "UNKNOWN",
			},
			expected: EntityForI2{
				entityId:     "e-1",
				entityType:   "Person",
				entityIcon:   "UNKNOWN-ICON",
				entityLabel:  "UNKNOWN-LABEL",
				isSeedEntity: "FALSE",
			},
			errorExpected: false,
		},
		{
			// Entity doesn't exist in bipartite graph store
			entityId:     "e-10",
			entityIsSeed: false,
			config: SpiderI2ChartConfig{
				EntityConfig: map[string]SpiderEntityConfig{
					"Address": {
						Icon:  "House",
						Label: "<First line>",
					},
				},
				UnknownEntityTypeIcon:  "UNKNOWN-ICON",
				UnknownEntityTypeLabel: "UNKNOWN-LABEL",
				MissingAttribute:       "UNKNOWN",
			},
			expected:      EntityForI2{},
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := makeEntityForI2(bipartite,
			testCase.entityId, testCase.entityIsSeed, testCase.config)

		assert.Equal(t, testCase.expected, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestRowForI2(t *testing.T) {
	r := RowForI2{
		entity1: EntityForI2{
			entityId:     "1",
			entityType:   "2",
			entityIcon:   "3",
			entityLabel:  "4",
			isSeedEntity: "5",
		},
		entity2: EntityForI2{
			entityId:     "6",
			entityType:   "7",
			entityIcon:   "8",
			entityLabel:  "9",
			isSeedEntity: "10",
		},
	}

	expected := []string{"1", "2", "3", "4", "5",
		"6", "7", "8", "9", "10"}
	actual := r.Serialise()
	assert.Equal(t, expected, actual)
}

func TestMakeSpiderRow(t *testing.T) {

	// Construct an in-memory bipartite graph store for the test
	bipartite := makeBipartiteStore(t)

	config := SpiderI2ChartConfig{
		EntityConfig: map[string]SpiderEntityConfig{
			"Person": {
				Icon:  "Anonymous",
				Label: "<Full Name>",
			},
		},
		UnknownEntityTypeIcon:  "UNKNOWN-ICON",
		UnknownEntityTypeLabel: "UNKNOWN-LABEL",
		MissingAttribute:       "UNKNOWN",
	}

	testCases := []struct {
		entityId        string
		entityIsSeed    bool
		adjEntityId     string
		adjEntityIsSeed bool
		config          SpiderI2ChartConfig
		expected        RowForI2
		errorExpected   bool
	}{
		{
			entityId:        "e-1",
			entityIsSeed:    true,
			adjEntityId:     "e-2",
			adjEntityIsSeed: false,
			config:          config,
			expected: RowForI2{
				entity1: EntityForI2{
					entityId:     "e-1",
					entityType:   "Person",
					entityIcon:   "Anonymous",
					entityLabel:  "Bob Smith",
					isSeedEntity: "TRUE",
				},
				entity2: EntityForI2{
					entityId:     "e-2",
					entityType:   "Person",
					entityIcon:   "Anonymous",
					entityLabel:  "Sally Jones",
					isSeedEntity: "FALSE",
				},
			},
			errorExpected: false,
		},
		{
			// Entity 1 is missing from bipartite graph
			entityId:        "e-10",
			entityIsSeed:    true,
			adjEntityId:     "e-1",
			adjEntityIsSeed: false,
			config:          config,
			expected:        RowForI2{},
			errorExpected:   true,
		},
		{
			// Entity 2 is missing from bipartite graph
			entityId:        "e-1",
			entityIsSeed:    true,
			adjEntityId:     "e-10",
			adjEntityIsSeed: false,
			config:          config,
			expected:        RowForI2{},
			errorExpected:   true,
		},
	}

	for _, testCase := range testCases {
		actual, err := makeSpiderRow(bipartite, testCase.entityId,
			testCase.entityIsSeed, testCase.adjEntityId, testCase.adjEntityIsSeed,
			testCase.config)

		assert.Equal(t, testCase.expected, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestBuildChart(t *testing.T) {

	// Construct an in-memory bipartite graph store for the test
	bipartite := makeBipartiteStore(t)

	// Instantiate a spider chart builder
	s, err := NewSpiderChartBuilder("./test-data/spider-i2-config-1.json")
	assert.NoError(t, err)
	s.SetBipartite(bipartite)

	// Sub-graph for test case 1
	subgraph1 := graphstore.NewInMemoryUnipartiteGraphStore()
	subgraph1.AddUndirected("e-1", "e-2")

	// Sub-graph for test case 2
	subgraph2 := graphstore.NewInMemoryUnipartiteGraphStore()
	subgraph2.AddUndirected("e-1", "e-2")
	subgraph2.AddUndirected("e-1", "e-3")

	testCases := []struct {
		results       *spider.SpiderResults
		expected      [][]string
		errorExpected bool
	}{
		{
			// Spider results is nil
			results:       nil,
			expected:      nil,
			errorExpected: true,
		},
		{
			// One connection
			results: &spider.SpiderResults{
				NumberSteps:          1,
				Subgraph:             subgraph1,
				SeedEntities:         set.NewPopulatedSet("e-1"),
				SeedEntitiesNotFound: set.NewSet[string](),
			},
			expected: [][]string{
				{"ID-1", "Type-1", "Icon-1", "Label-1", "Seed-1", "ID-2", "Type-2", "Icon-2", "Label-2", "Seed-2"},
				{"e-1", "Person", "Anonymous", "Bob Smith", "TRUE", "e-2", "Person", "Anonymous", "Sally Jones", "FALSE"},
			},
			errorExpected: false,
		},
		{
			// Two connections
			results: &spider.SpiderResults{
				NumberSteps:          1,
				Subgraph:             subgraph2,
				SeedEntities:         set.NewPopulatedSet("e-1", "e-3"),
				SeedEntitiesNotFound: set.NewSet[string](),
			},
			expected: [][]string{
				{"ID-1", "Type-1", "Icon-1", "Label-1", "Seed-1", "ID-2", "Type-2", "Icon-2", "Label-2", "Seed-2"},
				{"e-1", "Person", "Anonymous", "Bob Smith", "TRUE", "e-2", "Person", "Anonymous", "Sally Jones", "FALSE"},
				{"e-1", "Person", "Anonymous", "Bob Smith", "TRUE", "e-3", "Person", "Anonymous", "Sandra Jackson", "TRUE"},
			},
			errorExpected: false,
		},
	}

	for _, testCase := range testCases {
		actual, err := s.Build(testCase.results)

		assert.Equal(t, testCase.expected, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
