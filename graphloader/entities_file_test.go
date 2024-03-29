package graphloader

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestEntitiesHeaderOnly(t *testing.T) {

	csv := NewEntitiesCsvFile("./test-data/entities_0.csv", "Person", ",",
		"entity_id", map[string]string{
			"first name": "Forename",
			"last name":  "Surname",
		})

	reader := NewEntitiesCsvFileReader(csv)

	err := reader.Initialise()
	assert.NoError(t, err)

	assert.Equal(t, 0, reader.entityIdFieldIndex)
	assert.Equal(t, map[string]int{
		"Forename": 1,
		"Surname":  2,
	}, reader.attributeFieldIndex)

	assert.False(t, reader.hasNext)
	assert.NoError(t, reader.Close())
	assert.Equal(t, 1, reader.numberOfRows)
	assert.Equal(t, 0, reader.numberOfEntities)
}

func TestReadEntitiesFile(t *testing.T) {
	testCases := []struct {
		csv                    EntitiesCsvFile
		expectedEntities       []graphstore.Entity
		expectedError          bool
		expectedNumberRows     int
		expectedNumberEntities int
	}{
		{
			csv: EntitiesCsvFile{
				Path:          "./test-data/entities_0.csv",
				EntityType:    "Person",
				Delimiter:     ",",
				EntityIdField: "entity_id",
				FieldToAttribute: map[string]string{
					"first name": "Forename",
					"last name":  "Surname",
				},
			},
			expectedEntities:       []graphstore.Entity{},
			expectedError:          false,
			expectedNumberRows:     1,
			expectedNumberEntities: 0,
		},
		{
			csv: EntitiesCsvFile{
				Path:          "./test-data/entities_1.csv",
				EntityType:    "Person",
				Delimiter:     ",",
				EntityIdField: "entity_id",
				FieldToAttribute: map[string]string{
					"first name": "Forename",
					"last name":  "Surname",
				},
			},
			expectedEntities: []graphstore.Entity{
				{
					Id:         "e-1",
					EntityType: "Person",
					Attributes: map[string]string{
						"Forename": "Bob",
						"Surname":  "Smith",
					},
					LinkedDocumentIds: set.NewSet[string](),
				},
			},
			expectedError:          false,
			expectedNumberRows:     2,
			expectedNumberEntities: 1,
		},
		{
			csv: EntitiesCsvFile{
				Path:          "./test-data/entities_2.csv",
				EntityType:    "Person",
				Delimiter:     ",",
				EntityIdField: "entity_id",
				FieldToAttribute: map[string]string{
					"first name": "Forename",
					"last name":  "Surname",
				},
			},
			expectedEntities: []graphstore.Entity{
				{
					Id:         "e-1",
					EntityType: "Person",
					Attributes: map[string]string{
						"Forename": "Bob",
						"Surname":  "Smith",
					},
					LinkedDocumentIds: set.NewSet[string](),
				},
				{
					Id:         "e-2",
					EntityType: "Person",
					Attributes: map[string]string{
						"Forename": "Sally",
						"Surname":  "Jones",
					},
					LinkedDocumentIds: set.NewSet[string](),
				},
			},
			expectedError:          false,
			expectedNumberRows:     3,
			expectedNumberEntities: 2,
		},
		{
			// The file is missing the 'age' field
			csv: EntitiesCsvFile{
				Path:          "./test-data/entities_3.csv",
				EntityType:    "Person",
				Delimiter:     ",",
				EntityIdField: "entity_id",
				FieldToAttribute: map[string]string{
					"first name": "Forename",
					"last name":  "Surname",
					"age":        "Age",
				},
			},
			expectedEntities:       nil,
			expectedError:          true,
			expectedNumberRows:     1,
			expectedNumberEntities: 0,
		},
		{
			csv: EntitiesCsvFile{
				Path:          "./test-data/entities_4.csv",
				EntityType:    "Person",
				Delimiter:     "|",
				EntityIdField: "entity_id",
				FieldToAttribute: map[string]string{
					"first name": "Forename",
					"last name":  "Surname",
				},
			},
			expectedEntities: []graphstore.Entity{
				{
					Id:         "e-1",
					EntityType: "Person",
					Attributes: map[string]string{
						"Forename": "Bob",
						"Surname":  "Smith",
					},
					LinkedDocumentIds: set.NewSet[string](),
				},
				{
					Id:         "e-2",
					EntityType: "Person",
					Attributes: map[string]string{
						"Forename": "Sally|Kat",
						"Surname":  "Jones",
					},
					LinkedDocumentIds: set.NewSet[string](),
				},
			},
			expectedError:          false,
			expectedNumberRows:     3,
			expectedNumberEntities: 2,
		},
	}

	for _, testCase := range testCases {
		reader := NewEntitiesCsvFileReader(testCase.csv)

		entities, err := reader.ReadAll()

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedEntities, entities)
		assert.Equal(t, testCase.expectedNumberRows, reader.numberOfRows)
		assert.Equal(t, testCase.expectedNumberEntities, reader.numberOfEntities)
	}
}
