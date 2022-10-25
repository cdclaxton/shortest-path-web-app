package graphloader

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestDocumentsHeaderOnly(t *testing.T) {

	csv := NewDocumentsCsvFile("./test-data/documents_0.csv", "Source-1",
		",", "document_id", map[string]string{
			"title": "Title",
			"date":  "Date",
		})

	reader := NewDocumentsCsvFileReader(csv)

	err := reader.Initialise()
	assert.NoError(t, err)

	assert.Equal(t, 0, reader.documentIdFieldIndex)
	assert.Equal(t, map[string]int{
		"Title": 1,
		"Date":  2,
	}, reader.attributeFieldIndex)

	assert.False(t, reader.hasNext)
	assert.NoError(t, reader.Close())

	assert.Equal(t, 1, reader.numberOfRows)
	assert.Equal(t, 0, reader.numberOfDocuments)
}

func TestReadDocumentsFile(t *testing.T) {
	testCases := []struct {
		csv                     DocumentsCsvFile
		expectedDocuments       []graphstore.Document
		expectedError           bool
		expectedNumberRows      int
		expectedNumberDocuments int
	}{
		{
			csv: DocumentsCsvFile{
				Path:            "./test-data/documents_0.csv",
				DocumentType:    "Source-1",
				Delimiter:       ",",
				DocumentIdField: "document_id",
				FieldToAttribute: map[string]string{
					"title": "Title",
					"date":  "Date",
				},
			},
			expectedDocuments:       []graphstore.Document{},
			expectedError:           false,
			expectedNumberRows:      1,
			expectedNumberDocuments: 0,
		},
		{
			csv: DocumentsCsvFile{
				Path:            "./test-data/documents_1.csv",
				DocumentType:    "Source-1",
				Delimiter:       ",",
				DocumentIdField: "document_id",
				FieldToAttribute: map[string]string{
					"title": "Title",
					"date":  "Date",
				},
			},
			expectedDocuments: []graphstore.Document{
				{
					Id:           "d-1",
					DocumentType: "Source-1",
					Attributes: map[string]string{
						"Title": "A summary of activity",
						"Date":  "06/08/2022",
					},
					LinkedEntityIds: set.NewSet[string](),
				},
			},
			expectedError:           false,
			expectedNumberRows:      2,
			expectedNumberDocuments: 1,
		},
		{
			csv: DocumentsCsvFile{
				Path:            "./test-data/documents_2.csv",
				DocumentType:    "Source-1",
				Delimiter:       ",",
				DocumentIdField: "document_id",
				FieldToAttribute: map[string]string{
					"title": "Title",
					"date":  "Date",
				},
			},
			expectedDocuments: []graphstore.Document{
				{
					Id:           "d-1",
					DocumentType: "Source-1",
					Attributes: map[string]string{
						"Title": "A summary of activity",
						"Date":  "06/08/2022",
					},
					LinkedEntityIds: set.NewSet[string](),
				},
				{
					Id:           "d-2",
					DocumentType: "Source-1",
					Attributes: map[string]string{
						"Title": "New information",
						"Date":  "07/08/2022",
					},
					LinkedEntityIds: set.NewSet[string](),
				},
			},
			expectedError:           false,
			expectedNumberRows:      3,
			expectedNumberDocuments: 2,
		},
		{
			csv: DocumentsCsvFile{
				Path:            "./test-data/documents_0.csv",
				DocumentType:    "Source-1",
				Delimiter:       ",",
				DocumentIdField: "document_id",
				FieldToAttribute: map[string]string{
					"title":  "Title",
					"date":   "Date",
					"serial": "Serial",
				},
			},
			expectedDocuments:       nil,
			expectedError:           true,
			expectedNumberRows:      1,
			expectedNumberDocuments: 0,
		},
	}

	for _, testCase := range testCases {
		reader := NewDocumentsCsvFileReader(testCase.csv)

		documents, err := reader.ReadAll()

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedDocuments, documents)
		assert.Equal(t, testCase.expectedNumberRows, reader.numberOfRows)
		assert.Equal(t, testCase.expectedNumberDocuments, reader.numberOfDocuments)
	}
}
