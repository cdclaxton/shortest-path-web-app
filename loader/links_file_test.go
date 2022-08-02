package loader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeaderOnly(t *testing.T) {

	csv := NewLinksCsvFile("./test-data/links_0.csv", "entity_id", "document_id", ",")
	reader := NewLinksCsvFileReader(csv)

	err := reader.Initialise()
	assert.NoError(t, err)

	assert.Equal(t, 0, reader.entityIdFieldIndex)
	assert.Equal(t, 1, reader.documentIdFieldIndex)
	assert.False(t, reader.hasNext)

	assert.NoError(t, reader.Close())
}

func TestReadFile(t *testing.T) {

	testCases := []struct {
		path            string
		entityIdField   string
		documentIdField string
		delimiter       string
		expected        []Link
	}{
		{
			path:            "./test-data/links_1.csv",
			entityIdField:   "entity_id",
			documentIdField: "document_id",
			delimiter:       ",",
			expected: []Link{
				{
					EntityId:   "e-100",
					DocumentId: "d-3",
				},
			},
		},
		{
			path:            "./test-data/links_2.csv",
			entityIdField:   "entity_id",
			documentIdField: "document_id",
			delimiter:       ",",
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
		},
		{
			path:            "./test-data/links_3.csv",
			entityIdField:   "entity_id",
			documentIdField: "document_id",
			delimiter:       ",",
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
		},
	}

	for _, testCase := range testCases {
		csv := NewLinksCsvFile(testCase.path, testCase.entityIdField,
			testCase.documentIdField, testCase.delimiter)

		reader := NewLinksCsvFileReader(csv)

		links, err := reader.ReadAll()
		assert.NoError(t, err)

		assert.Equal(t, testCase.expected, links)
	}

}
