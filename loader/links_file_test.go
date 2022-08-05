package loader

import (
	"testing"

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
}

func TestReadLinksFile(t *testing.T) {

	testCases := []struct {
		csv           LinksCsvFile
		expected      []Link
		expectedError bool
	}{
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_1.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
			},
			expectedError: false,
		},
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_2.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
				{
					EntityId:   "e-101",
					DocumentId: "d-4",
				},
			},
			expectedError: false,
		},
		{
			csv: LinksCsvFile{
				Path:            "./test-data/links_3.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected: []Link{
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
			expectedError: false,
		},
		{
			// CSV file is missing the entity ID field
			csv: LinksCsvFile{
				Path:            "./test-data/links_4.csv",
				EntityIdField:   "entity_id",
				DocumentIdField: "document_id",
				Delimiter:       ",",
			},
			expected:      nil,
			expectedError: true,
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
	}

}
