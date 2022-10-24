package loader

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/stretchr/testify/assert"
)

func TestLinksHeaderOnly(t *testing.T) {

	csv := NewLinksCsvFile("./test-data/links_0.csv", "entity_id", "document_id", ",")
	reader := NewLinksCsvFileReader(csv)

	err := reader.Initialise()
	assert.NoError(t, err)

	assert.Equal(t, 0, reader.entityIdFieldIndex)
	assert.Equal(t, 1, reader.documentIdFieldIndex)
	assert.False(t, reader.hasNext)

	assert.NoError(t, reader.Close())
	assert.Equal(t, 1, reader.numberOfRows)
	assert.Equal(t, 0, reader.numberOfLinks)
}

func TestReadLinksFile(t *testing.T) {

	testCases := []struct {
		csv                 LinksCsvFile
		expected            []graphstore.Link
		expectedError       bool
		expectedNumberRows  int
		expectedNumberLinks int
	}{
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_1.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []graphstore.Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
			},
			expectedError:       false,
			expectedNumberRows:  2,
			expectedNumberLinks: 1,
		},
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_2.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []graphstore.Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
				{
					EntityId:   "e-101",
					DocumentId: "d-4",
				},
			},
			expectedError:       false,
			expectedNumberRows:  3,
			expectedNumberLinks: 2,
		},
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_3.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []graphstore.Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
				{
					EntityId:   "e-101",
					DocumentId: "d-4",
				},
				{
					EntityId:   "e-200",
					DocumentId: "d-10",
				},
			},
			expectedError:       false,
			expectedNumberRows:  5, // contains a broken row
			expectedNumberLinks: 3,
		},
		{
			// CSV file is missing the entity ID field
			csv: LinksCsvFile{
				Path:            "./test-data/links_4.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected:            nil,
			expectedError:       true,
			expectedNumberRows:  0,
			expectedNumberLinks: 0,
		},
	}

	for _, testCase := range testCases {
		reader := NewLinksCsvFileReader(testCase.csv)

		links, err := reader.ReadAll()

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expected, links)
		assert.Equal(t, testCase.expectedNumberRows, reader.numberOfRows)
		assert.Equal(t, testCase.expectedNumberLinks, reader.numberOfLinks)
	}

}
