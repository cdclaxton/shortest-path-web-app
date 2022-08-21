package graphbuilder

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/loader"
	"github.com/stretchr/testify/assert"
)

func TestReadGraphDataConfigWhenFileDoesNotExist(t *testing.T) {

	// Read the file
	filepath := "../test-data/sets/config.json"
	_, err := readGraphDataConfig(filepath)
	assert.Error(t, err)
}

func TestReadGraphDataConfigValidFile(t *testing.T) {

	// Read the file
	filepath := "../test-data-sets/set-0/config.json"
	graphData, err := readGraphDataConfig(filepath)
	assert.NoError(t, err)

	// Check the entity files
	expectedEntityFiles := []loader.EntitiesCsvFile{
		{
			Path:          "entities_0.csv",
			EntityType:    "Person",
			Delimiter:     ",",
			EntityIdField: "entity ID",
			FieldToAttribute: map[string]string{
				"Name": "Full Name",
			},
		},
		{
			Path:          "entities_1.csv",
			EntityType:    "Person",
			Delimiter:     ",",
			EntityIdField: "ENTITY ID",
			FieldToAttribute: map[string]string{
				"NAME": "Full Name",
			},
		},
	}
	assert.Equal(t, expectedEntityFiles, graphData.EntitiesFiles)

	// Check the document files
	expectedDocumentsFiles := []loader.DocumentsCsvFile{
		{
			Path:            "documents_0.csv",
			DocumentType:    "Doc-type-A",
			Delimiter:       ",",
			DocumentIdField: "document ID",
			FieldToAttribute: map[string]string{
				"title": "Title",
				"date":  "Date",
			},
		},
		{
			Path:            "documents_1.csv",
			DocumentType:    "Doc-type-B",
			Delimiter:       ",",
			DocumentIdField: "DOCUMENT ID",
			FieldToAttribute: map[string]string{
				"title": "Title",
				"date":  "Date",
			},
		},
	}
	assert.Equal(t, expectedDocumentsFiles, graphData.DocumentsFiles)

	// Check the link files
	expectedLinksFiles := []loader.LinksCsvFile{
		{
			Path:            "links_0.csv",
			EntityIdField:   "entity ID",
			DocumentIdField: "document ID",
			Delimiter:       ",",
		},
		{
			Path:            "links_1.csv",
			EntityIdField:   "ENTITY ID",
			DocumentIdField: "DOCUMENT ID",
			Delimiter:       ",",
		},
	}
	assert.Equal(t, expectedLinksFiles, graphData.LinksFiles)

	// Check the skip file
	assert.Equal(t, "skip_entities.txt", graphData.SkipEntitiesFile)
}

func TestGraphBuilderValidConfig(t *testing.T) {

}
