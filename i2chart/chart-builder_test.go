package i2chart

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
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
		entity2 := bipartite.GetEntity(testCase.entityId2)

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
