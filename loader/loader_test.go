package loader

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

const testDataSetFolder = "../test-data-sets"

func TestFindIndicesOfFields(t *testing.T) {

	testCases := []struct {
		header        []string
		fields        []string
		expected      map[string]int
		errorExpected bool
	}{
		{
			header:        []string{"a"},
			fields:        []string{"a"},
			expected:      map[string]int{"a": 0},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: false,
		},
		{
			header:        []string{"b", "a"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 1, "b": 0},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b", "c"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b", "c"},
			fields:        []string{"a", "b", "d"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := findIndicesOfFields(testCase.header, testCase.fields)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestAttributeToFieldIndex(t *testing.T) {
	testCases := []struct {
		header           []string
		fieldToAttribute map[string]string
		expected         map[string]int
		errorExpected    bool
	}{
		{
			header:           []string{"name"},
			fieldToAttribute: map[string]string{"name": "Forename"},
			expected:         map[string]int{"Forename": 0},
			errorExpected:    false,
		},
		{
			header: []string{"first", "last"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected: map[string]int{
				"Forename": 0,
				"Surname":  1},
			errorExpected: false,
		},
		{
			header: []string{"last", "first"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected: map[string]int{
				"Forename": 1,
				"Surname":  0},
			errorExpected: false,
		},
		{
			header: []string{"last"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := attributeToFieldIndex(testCase.header, testCase.fieldToAttribute)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestExtractAttributes(t *testing.T) {
	testCases := []struct {
		header                []string
		attributeTofieldIndex map[string]int
		expected              map[string]string
		errorExpected         bool
	}{
		{
			header: []string{"Bob"},
			attributeTofieldIndex: map[string]int{
				"Forename": 0},
			expected: map[string]string{
				"Forename": "Bob",
			},
			errorExpected: false,
		},
		{
			header: []string{"Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 0,
				"Surname":  1},
			expected: map[string]string{
				"Forename": "Bob",
				"Surname":  "Smith",
			},
			errorExpected: false,
		},
		{
			header: []string{"23", "Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 1,
				"Surname":  2,
				"Age":      0},
			expected: map[string]string{
				"Age":      "23",
				"Forename": "Bob",
				"Surname":  "Smith",
			},
			errorExpected: false,
		},
		{
			header: []string{"Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 1,
				"Surname":  2,
				"Age":      0},
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := extractAttributes(testCase.header, testCase.attributeTofieldIndex)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestGraphStoreLoaderFromCsv(t *testing.T) {
	g := graphstore.NewInMemoryBipartiteGraphStore()

	entityFiles := []EntitiesCsvFile{
		{
			Path:          testDataSetFolder + "/set-0/data/entities_0.csv",
			EntityType:    "Person",
			Delimiter:     ",",
			EntityIdField: "entity ID",
			FieldToAttribute: map[string]string{
				"Name": "Name",
			},
		},
		{
			Path:          testDataSetFolder + "/set-0/data/entities_1.csv",
			EntityType:    "Person",
			Delimiter:     ",",
			EntityIdField: "ENTITY ID",
			FieldToAttribute: map[string]string{
				"NAME": "Name",
			},
		},
	}

	documentFiles := []DocumentsCsvFile{
		{
			Path:            testDataSetFolder + "/set-0/data/documents_0.csv",
			DocumentType:    "Source A",
			Delimiter:       ",",
			DocumentIdField: "document ID",
			FieldToAttribute: map[string]string{
				"title": "Title",
				"date":  "Date",
			},
		},
		{
			Path:            testDataSetFolder + "/set-0/data/documents_1.csv",
			DocumentType:    "Source B",
			Delimiter:       ",",
			DocumentIdField: "DOCUMENT ID",
			FieldToAttribute: map[string]string{
				"title": "Title",
				"date":  "Date",
			},
		},
	}

	linksFiles := []LinksCsvFile{
		{
			Path:            testDataSetFolder + "/set-0/data/links_0.csv",
			EntityIdField:   "entity ID",
			DocumentIdField: "document ID",
			Delimiter:       ",",
		},
		{
			Path:            testDataSetFolder + "/set-0/data/links_1.csv",
			EntityIdField:   "ENTITY ID",
			DocumentIdField: "DOCUMENT ID",
			Delimiter:       ",",
		},
	}

	loader := NewGraphStoreLoaderFromCsv(g, entityFiles, documentFiles, linksFiles)

	err := loader.Load()
	assert.NoError(t, err)

	// Check the entities
	nEntities, err := g.NumberOfEntities()
	assert.NoError(t, err)
	assert.Equal(t, 4, nEntities)

	expectedEntities := []graphstore.Entity{
		{
			Id:         "e-1",
			EntityType: "Person",
			Attributes: map[string]string{
				"Name": "Bob Smith",
			},
			LinkedDocumentIds: set.NewPopulatedSet("d-1", "d-2", "d-3"),
		},
		{
			Id:         "e-2",
			EntityType: "Person",
			Attributes: map[string]string{
				"Name": "Sally Jones",
			},
			LinkedDocumentIds: set.NewPopulatedSet("d-1", "d-2"),
		},
		{
			Id:         "e-3",
			EntityType: "Person",
			Attributes: map[string]string{
				"Name": "Sandra Jackson",
			},
			LinkedDocumentIds: set.NewPopulatedSet("d-3", "d-4"),
		},
		{
			Id:         "e-4",
			EntityType: "Person",
			Attributes: map[string]string{
				"Name": "Samuel Taylor",
			},
			LinkedDocumentIds: set.NewPopulatedSet("d-4"),
		},
	}

	for _, expectedEntity := range expectedEntities {
		assert.True(t, g.HasEntity(&expectedEntity))
	}

	// Check the documents
	nDocuments, err := g.NumberOfDocuments()
	assert.NoError(t, err)
	assert.Equal(t, 4, nDocuments)

	expectedDocuments := []graphstore.Document{
		{
			Id:           "d-1",
			DocumentType: "Source A",
			Attributes: map[string]string{
				"Title": "Summary 1",
				"Date":  "06/08/2022",
			},
			LinkedEntityIds: set.NewPopulatedSet("e-1", "e-2"),
		},
		{
			Id:           "d-2",
			DocumentType: "Source A",
			Attributes: map[string]string{
				"Title": "Summary 2",
				"Date":  "07/08/2022",
			},
			LinkedEntityIds: set.NewPopulatedSet("e-1", "e-2"),
		},
		{
			Id:           "d-3",
			DocumentType: "Source B",
			Attributes: map[string]string{
				"Title": "Summary 3",
				"Date":  "09/08/2022",
			},
			LinkedEntityIds: set.NewPopulatedSet("e-1", "e-3"),
		},
		{
			Id:           "d-4",
			DocumentType: "Source B",
			Attributes: map[string]string{
				"Title": "Summary 4",
				"Date":  "10/08/2022",
			},
			LinkedEntityIds: set.NewPopulatedSet("e-3", "e-4"),
		},
	}

	for _, expectedDocument := range expectedDocuments {
		assert.True(t, g.HasDocument(&expectedDocument))
	}
}
