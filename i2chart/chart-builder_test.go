package i2chart

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestReadI2Config(t *testing.T) {
	filepath := "./test-data/i2-config-1.json"

	config, err := readI2Config(filepath)
	assert.NoError(t, err)
	assert.NotNil(t, config)
}

func TestValidateI2Config(t *testing.T) {
	testCases := []struct {
		filepath      string
		isValid       bool
		numberReasons int
	}{
		{
			filepath:      "./test-data/i2-config-1.json",
			isValid:       true,
			numberReasons: 0,
		},
		{
			filepath:      "./test-data/i2-invalid-config-1.json",
			isValid:       false,
			numberReasons: 2,
		},
		{
			filepath:      "./test-data/i2-invalid-config-2.json",
			isValid:       false,
			numberReasons: 1,
		},
		{
			filepath:      "./test-data/i2-invalid-config-3.json",
			isValid:       false,
			numberReasons: 1,
		},
		{
			filepath:      "./test-data/i2-invalid-config-4.json",
			isValid:       false,
			numberReasons: 1,
		},
	}

	for _, testCase := range testCases {

		// Read the JSON file
		config, err := readI2Config(testCase.filepath)
		assert.NoError(t, err)
		assert.NotNil(t, config)

		// Validate the config
		valid, reasons := validateI2Config(*config)
		assert.Equal(t, testCase.isValid, valid)
		assert.Equal(t, testCase.numberReasons, len(reasons))
	}
}

func TestHeader(t *testing.T) {
	testCases := []struct {
		columns  []string
		expected []string
	}{
		{
			columns:  []string{"Name"},
			expected: []string{"Entity-Name-1", "Entity-Name-2", "Link"},
		},
		{
			columns: []string{"Name", "Dob"},
			expected: []string{"Entity-Name-1", "Entity-Dob-1",
				"Entity-Name-2", "Entity-Dob-2", "Link"},
		},
	}

	for _, testCase := range testCases {
		actual := header(testCase.columns)
		assert.Equal(t, testCase.expected, actual)
	}
}

// makeBipartiteStore to use in the tests.
func makeBipartiteStore(t *testing.T) graphstore.BipartiteGraphStore {

	filepath := "../test-data-sets/set-0/config.json"
	builder, err := graphbuilder.NewGraphBuilderFromJson(filepath)
	assert.NoError(t, err)
	return builder.Bipartite
}

func TestDocumentsLinkingEntities(t *testing.T) {

	bipartite := makeBipartiteStore(t)

	testCases := []struct {
		entityId1           string
		entityId2           string
		expectedDocumentIds []string
		expectedError       bool
	}{
		{
			entityId1:           "e-1",
			entityId2:           "e-2",
			expectedDocumentIds: []string{"d-1", "d-2"},
			expectedError:       false,
		},
		{
			entityId1:           "e-1",
			entityId2:           "e-3",
			expectedDocumentIds: []string{"d-3"},
			expectedError:       false,
		},
		{
			entityId1:           "e-1",
			entityId2:           "e-4",
			expectedDocumentIds: []string{""},
			expectedError:       true,
		},
	}

	for _, testCase := range testCases {

		// Get the entities from the bipartite store
		entity1 := bipartite.GetEntity(testCase.entityId1)
		assert.NotNil(t, entity1)
		entity2 := bipartite.GetEntity(testCase.entityId2)
		assert.NotNil(t, entity2)

		actualDocs, err := documentsLinkingEntities(entity1, entity2, bipartite)
		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, len(testCase.expectedDocumentIds), len(actualDocs))

			for idx := range testCase.expectedDocumentIds {
				assert.Equal(t, testCase.expectedDocumentIds[idx],
					actualDocs[idx].Id)
			}
		}
	}
}

func TestSubstituteForLink(t *testing.T) {
	testCases := []struct {
		docs             []*graphstore.Document
		spec             LinksSpec
		missingAttribute string
		expectedLabel    string
	}{
		{
			docs: []*graphstore.Document{
				{
					DocumentType: "Type-A",
					Attributes:   map[string]string{"date": "06/09/2022"},
				},
			},
			spec: LinksSpec{
				Label:         "<NUM-DOCS>; <DOCUMENT-TYPES>; <DOCUMENT-DATE-RANGE>",
				DateAttribute: "date",
				DateFormat:    "02/01/2006",
			},
			missingAttribute: "MISSING",
			expectedLabel:    "1; Type-A; 06/09/2022",
		},
		{
			docs: []*graphstore.Document{
				{
					DocumentType: "Type-A",
					Attributes:   map[string]string{"date": "06/09/2022"},
				},
				{
					DocumentType: "Type-B",
					Attributes:   map[string]string{"date": "05/09/2022"},
				},
			},
			spec: LinksSpec{
				Label:         "<NUM-DOCS>; <DOCUMENT-TYPES>; <DOCUMENT-DATE-RANGE>",
				DateAttribute: "date",
				DateFormat:    "02/01/2006",
			},
			missingAttribute: "MISSING",
			expectedLabel:    "2; Type-A, Type-B; 05/09/2022 - 06/09/2022",
		},
		{
			docs: []*graphstore.Document{
				{
					DocumentType: "Type-A",
					Attributes:   map[string]string{"date": "06/09/2022"},
				},
				{
					DocumentType: "Type-B",
					Attributes:   map[string]string{"date": "05/09/2022"},
				},
			},
			spec: LinksSpec{
				Label:         "<NUM-DOCS>; <DOCUMENT-TYPES>; <DOCUMENT-DATE-RANGE>; <COMMENT>",
				DateAttribute: "date",
				DateFormat:    "02/01/2006",
			},
			missingAttribute: "MISSING",
			expectedLabel:    "2; Type-A, Type-B; 05/09/2022 - 06/09/2022; MISSING",
		},
	}

	for _, testCase := range testCases {
		actual, err := substituteForLink(testCase.docs, testCase.spec, testCase.missingAttribute)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedLabel, actual)
	}
}

func TestMakeLinkLabel(t *testing.T) {

	// Make a bipartite store for the test
	bipartite := makeBipartiteStore(t)

	spec := LinksSpec{
		Label:         "<NUM-DOCS>; <DOCUMENT-TYPES>; <DOCUMENT-DATE-RANGE>",
		DateAttribute: "Date",
		DateFormat:    "02/01/2006",
	}
	missingAttribute := "MISSING"

	testCases := []struct {
		entityId1     string
		entityId2     string
		expectedLabel string
	}{
		{
			entityId1:     "e-1",
			entityId2:     "e-2",
			expectedLabel: "2; Doc-type-A; 06/08/2022 - 07/08/2022",
		},
		{
			entityId1:     "e-1",
			entityId2:     "e-3",
			expectedLabel: "1; Doc-type-B; 09/08/2022",
		},
		{
			entityId1:     "e-3",
			entityId2:     "e-4",
			expectedLabel: "1; Doc-type-B; 10/08/2022",
		},
	}

	for _, testCase := range testCases {

		// Get the entities
		entity1 := bipartite.GetEntity(testCase.entityId1)
		assert.NotNil(t, entity1)
		entity2 := bipartite.GetEntity(testCase.entityId2)
		assert.NotNil(t, entity2)

		// Make the link label
		actual, err := makeLinkLabel(entity1, entity2, bipartite, spec, missingAttribute)
		assert.NoError(t, err)

		// Check the label
		assert.Equal(t, testCase.expectedLabel, actual)
	}

}

func TestMakeI2Entity(t *testing.T) {

	entity := graphstore.Entity{
		Id:         "e-1",
		EntityType: "Person",
		Attributes: map[string]string{
			"Forename": "Bob",
			"Surname":  "Smith",
			"Age":      "23",
		},
	}

	keywords := map[string]string{
		"ENTITY-SET-NAMES": "E-Set-1",
	}
	missingAttribute := "MISSING"

	testCases := []struct {
		columns         []string
		entitySpec      map[string]map[string]string
		expectedColumns []string
		expectedError   bool
	}{
		{
			// Entity type is missing in the entity spec
			columns: []string{"Name"},
			entitySpec: map[string]map[string]string{
				"Address": {},
			},
			expectedColumns: nil,
			expectedError:   true,
		},
		{
			// One column
			columns: []string{"Name"},
			entitySpec: map[string]map[string]string{
				"Person": {
					"Name": "<Forename> <Surname>",
				},
			},
			expectedColumns: []string{"Bob Smith"},
			expectedError:   false,
		},
		{
			// Two columns, one missing from the spec
			columns: []string{"Name", "Age"},
			entitySpec: map[string]map[string]string{
				"Person": {
					"Name": "<Forename> <Surname>",
				},
			},
			expectedColumns: nil,
			expectedError:   true,
		},
		{
			// Two columns
			columns: []string{"Name", "Age"},
			entitySpec: map[string]map[string]string{
				"Person": {
					"Name": "<Forename> <Surname>",
					"Age":  "Age is <Age>",
				},
			},
			expectedColumns: []string{"Bob Smith", "Age is 23"},
			expectedError:   false,
		},
		{
			// Three columns, missing attribute
			columns: []string{"Name", "Age", "Address"},
			entitySpec: map[string]map[string]string{
				"Person": {
					"Name":    "<Forename> <Surname>",
					"Age":     "Age is <Age>",
					"Address": "Address is <Address>",
				},
			},
			expectedColumns: []string{"Bob Smith", "Age is 23", "Address is MISSING"},
			expectedError:   false,
		},
	}

	for _, testCase := range testCases {
		actual, err := makeI2Entity(&entity, testCase.columns, testCase.entitySpec,
			missingAttribute, keywords)

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedColumns, actual)
	}
}

func TestRowLinkingEntities(t *testing.T) {

	// Make the bipartite graph store
	dataFilepath := "../test-data-sets/set-1/data-config.json"
	graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(dataFilepath)
	assert.NoError(t, err)

	// Make the i2 chart builder
	filepath := "../test-data-sets/set-1/i2-config.json"
	chartBuilder, err := NewI2ChartBuilder(filepath)
	assert.NoError(t, err)

	// Inject the chart builder's dependency on the bipartite store
	chartBuilder.SetBipartite(graphBuilder.Bipartite)

	keywordToValue1 := map[string]string{
		"ENTITY-SET-NAMES": "Set-A",
	}
	keywordToValue2 := map[string]string{
		"ENTITY-SET-NAMES": "",
	}

	testCases := []struct {
		entityId1     string
		entityId2     string
		expectedError bool
		expectedRow   []string
	}{
		{
			entityId1:     "e-1",
			entityId2:     "e-2",
			expectedError: false,
			expectedRow: []string{
				"Person", "e-1", "Smith, Bob [Set-A]", "Set-A", "Bob Smith can be found at http://network-display/e-1",
				"Person", "e-2", "Jones, Sally []", "", "Sally Jones can be found at http://network-display/e-2",
				"2 docs (Doc-A, Doc-B; 06/08/2022 - 07/08/2022)"},
		},
		{
			entityId1:     "e-1",
			entityId2:     "e-3",
			expectedError: false,
			expectedRow: []string{
				"Person", "e-1", "Smith, Bob [Set-A]", "Set-A", "Bob Smith can be found at http://network-display/e-1",
				"Location", "e-3", "31 Field Drive, EH36 5PB []", "", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3",
				"1 docs (Doc-A; 09/08/2022)"},
		},
		{
			entityId1:     "e-3",
			entityId2:     "e-4",
			expectedError: false,
			expectedRow: []string{
				"Location", "e-3", "31 Field Drive, EH36 5PB [Set-A]", "Set-A", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3",
				"Person", "e-4", "Taylor, Samuel []", "", "Samuel Taylor can be found at http://network-display/e-4",
				"1 docs (Doc-A; 10/08/2022)"},
		},
		{
			entityId1:     "e-1",
			entityId2:     "e-4",
			expectedError: true,
			expectedRow:   nil,
		},
	}

	for _, testCase := range testCases {
		row, err := chartBuilder.rowLinkingEntities(testCase.entityId1,
			testCase.entityId2, keywordToValue1, keywordToValue2)

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedRow, row)
	}
}

func TestBuildDatasetKeywords(t *testing.T) {

	conns := bfs.NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{
			"e-1": set.NewPopulatedSet("Set-A"),
			"e-2": set.NewPopulatedSet("Set-A", "Set-B"),
		},
	}

	testCases := []struct {
		entityId string
		expected map[string]string
	}{
		{
			entityId: "e-1",
			expected: map[string]string{
				entitySetNamesKeyword: "Set-A",
			},
		},
		{
			entityId: "e-2",
			expected: map[string]string{
				entitySetNamesKeyword: "Set-A, Set-B",
			},
		},
		{
			entityId: "e-3",
			expected: map[string]string{
				entitySetNamesKeyword: "",
			},
		},
	}

	for _, testCase := range testCases {
		actual, err := buildDatasetKeywords(testCase.entityId, &conns)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestBuild(t *testing.T) {

	// Make the bipartite graph store
	dataFilepath := "../test-data-sets/set-1/data-config.json"
	graphBuilder, err := graphbuilder.NewGraphBuilderFromJson(dataFilepath)
	assert.NoError(t, err)

	// Make the i2 chart builder
	filepath := "../test-data-sets/set-1/i2-config.json"
	chartBuilder, err := NewI2ChartBuilder(filepath)
	assert.NoError(t, err)

	// Inject the chart builder's dependency on the bipartite store
	chartBuilder.SetBipartite(graphBuilder.Bipartite)

	testCases := []struct {
		conns         *bfs.NetworkConnections
		expectedError bool
		expectedRows  [][]string
	}{
		{
			// Nil conns should fail the precondition
			conns:         nil,
			expectedError: true,
			expectedRows:  nil,
		},
		{
			// No connections, therefore just the header should be returned
			conns: &bfs.NetworkConnections{
				EntityIdToSetNames: map[string]*set.Set[string]{},
				Connections:        map[string]map[string][]bfs.Path{},
			},
			expectedError: false,
			expectedRows: [][]string{
				{"Entity-icon-1", "Entity-id-1", "Entity-label-1", "Entity-entitySets-1", "Entity-description-1",
					"Entity-icon-2", "Entity-id-2", "Entity-label-2", "Entity-entitySets-2", "Entity-description-2",
					"Link"}},
		},
		{
			// A single connection
			conns: &bfs.NetworkConnections{
				EntityIdToSetNames: map[string]*set.Set[string]{
					"e-1": set.NewPopulatedSet("Dataset-A"),
				},
				Connections: map[string]map[string][]bfs.Path{
					"e-1": {"e-2": {{
						Route: []string{"e-1", "e-2"},
					}}},
				},
			},
			expectedError: false,
			expectedRows: [][]string{
				{"Entity-icon-1", "Entity-id-1", "Entity-label-1", "Entity-entitySets-1", "Entity-description-1",
					"Entity-icon-2", "Entity-id-2", "Entity-label-2", "Entity-entitySets-2", "Entity-description-2",
					"Link"},
				{"Person", "e-1", "Smith, Bob [Dataset-A]", "Dataset-A", "Bob Smith can be found at http://network-display/e-1",
					"Person", "e-2", "Jones, Sally []", "", "Sally Jones can be found at http://network-display/e-2",
					"2 docs (Doc-A, Doc-B; 06/08/2022 - 07/08/2022)"}},
		},
		{
			// Two connections (but essentially a duplicate, so there should only be one row)
			conns: &bfs.NetworkConnections{
				EntityIdToSetNames: map[string]*set.Set[string]{
					"e-1": set.NewPopulatedSet("Dataset-A"),
				},
				Connections: map[string]map[string][]bfs.Path{
					"e-1": {"e-2": {{
						Route: []string{"e-1", "e-2"},
					}}},
					"e-2": {"e-1": {{
						Route: []string{"e-2", "e-1"},
					}}},
				},
			},
			expectedError: false,
			expectedRows: [][]string{
				{"Entity-icon-1", "Entity-id-1", "Entity-label-1", "Entity-entitySets-1", "Entity-description-1",
					"Entity-icon-2", "Entity-id-2", "Entity-label-2", "Entity-entitySets-2", "Entity-description-2",
					"Link"},
				{"Person", "e-1", "Smith, Bob [Dataset-A]", "Dataset-A", "Bob Smith can be found at http://network-display/e-1",
					"Person", "e-2", "Jones, Sally []", "", "Sally Jones can be found at http://network-display/e-2",
					"2 docs (Doc-A, Doc-B; 06/08/2022 - 07/08/2022)"}},
		},
		{
			// Path covering three entities
			conns: &bfs.NetworkConnections{
				EntityIdToSetNames: map[string]*set.Set[string]{
					"e-1": set.NewPopulatedSet("Dataset-A"),
					"e-4": set.NewPopulatedSet("Dataset-B"),
				},
				Connections: map[string]map[string][]bfs.Path{
					"e-1": {"e-3": {{
						Route: []string{"e-1", "e-3", "e-4"},
					}}},
				},
			},
			expectedError: false,
			expectedRows: [][]string{
				{"Entity-icon-1", "Entity-id-1", "Entity-label-1", "Entity-entitySets-1", "Entity-description-1",
					"Entity-icon-2", "Entity-id-2", "Entity-label-2", "Entity-entitySets-2", "Entity-description-2",
					"Link"},
				{"Person", "e-1", "Smith, Bob [Dataset-A]", "Dataset-A", "Bob Smith can be found at http://network-display/e-1",
					"Location", "e-3", "31 Field Drive, EH36 5PB []", "", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3",
					"1 docs (Doc-A; 09/08/2022)"},
				{"Location", "e-3", "31 Field Drive, EH36 5PB []", "", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3",
					"Person", "e-4", "Taylor, Samuel [Dataset-B]", "Dataset-B", "Samuel Taylor can be found at http://network-display/e-4",
					"1 docs (Doc-A; 10/08/2022)"}},
		},
		{
			// Invalid path (e-1 and e-4 are not connected directly)
			conns: &bfs.NetworkConnections{
				EntityIdToSetNames: map[string]*set.Set[string]{},
				Connections: map[string]map[string][]bfs.Path{
					"e-1": {"e-4": {{
						Route: []string{"e-1", "e-4"},
					}}},
				},
			},
			expectedError: true,
			expectedRows:  nil,
		},
	}

	for _, testCase := range testCases {
		actual, err := chartBuilder.Build(testCase.conns)

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedRows, actual)
		}

	}
}
