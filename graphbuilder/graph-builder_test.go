package graphbuilder

import (
	"path/filepath"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/loader"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestReadGraphConfigWhenFileDoesNotExist(t *testing.T) {

	// Read the file
	filepath := "../test-data/sets/config.json"
	_, err := readGraphConfig(filepath)
	assert.Error(t, err)
}

func TestReadGraphConfigValidFile(t *testing.T) {

	// Read the file
	filepath := "../test-data-sets/set-0/config.json"
	graphConfig, err := readGraphConfig(filepath)
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
	assert.Equal(t, expectedEntityFiles, graphConfig.Data.EntitiesFiles)

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
	assert.Equal(t, expectedDocumentsFiles, graphConfig.Data.DocumentsFiles)

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
	assert.Equal(t, expectedLinksFiles, graphConfig.Data.LinksFiles)

	// Check the skip file
	assert.Equal(t, "skip_entities.txt", graphConfig.Data.SkipEntitiesFile)
}

func TestMakePathRelative(t *testing.T) {
	testCases := []struct {
		dataFilename   string
		configFilepath string
		expectedPath   string
	}{
		{
			dataFilename:   "file1.csv",
			configFilepath: filepath.FromSlash("../../config.json"),
			expectedPath:   filepath.FromSlash("../../data/file1.csv"),
		},
		{
			dataFilename:   "file2.csv",
			configFilepath: filepath.FromSlash("./config/config.json"),
			expectedPath:   filepath.FromSlash("config/data/file2.csv"),
		},
		{
			dataFilename:   "file3.csv",
			configFilepath: filepath.FromSlash("./config.json"),
			expectedPath:   filepath.FromSlash("data/file3.csv"),
		},
		{
			dataFilename:   "file3.csv",
			configFilepath: filepath.FromSlash("config.json"),
			expectedPath:   filepath.FromSlash("data/file3.csv"),
		},
	}

	for _, testCase := range testCases {
		actual := makePathRelative(testCase.dataFilename, testCase.configFilepath)
		assert.Equal(t, testCase.expectedPath, actual)
	}
}

func TestMakePathsRelativeToConfig(t *testing.T) {

	graphConfig := GraphConfig{
		Data: GraphData{
			EntitiesFiles: []loader.EntitiesCsvFile{
				{
					Path: "entities_1.csv",
				},
			},
			DocumentsFiles: []loader.DocumentsCsvFile{
				{
					Path: "documents_1.csv",
				},
			},
			LinksFiles: []loader.LinksCsvFile{
				{
					Path: "links_1.csv",
				},
			},
			SkipEntitiesFile: "skip.txt",
		},
	}

	makePathsRelativeToConfig("../config/config.json", &graphConfig)

	// Check the entities files
	assert.Equal(t, filepath.FromSlash("../config/data/entities_1.csv"),
		graphConfig.Data.EntitiesFiles[0].Path)

	// Check the documents files
	assert.Equal(t, filepath.FromSlash("../config/data/documents_1.csv"),
		graphConfig.Data.DocumentsFiles[0].Path)

	// Check the links files
	assert.Equal(t, filepath.FromSlash("../config/data/links_1.csv"),
		graphConfig.Data.LinksFiles[0].Path)

	// Check the skip entities file
	assert.Equal(t, filepath.FromSlash("../config/data/skip.txt"),
		graphConfig.Data.SkipEntitiesFile)
}

func TestGraphBuilderValidConfig(t *testing.T) {

	// Read the JSON config file
	filepath := "../test-data-sets/set-0/config.json"
	graphConfig, err := readGraphConfig(filepath)
	assert.NoError(t, err)

	// Modify the data file paths to be based on the location of the config.json file
	makePathsRelativeToConfig(filepath, graphConfig)

	// Instantiate the graph builder
	graphBuilder, err := NewGraphBuilder(*graphConfig)
	assert.NoError(t, err)

	// Check the bipartite graph
	expectedBipartite := graphstore.NewInMemoryBipartiteGraphStore()

	documents := []graphstore.Document{
		{
			Id:           "d-1",
			DocumentType: "Doc-type-A",
			Attributes: map[string]string{
				"Title": "Summary 1",
				"Date":  "06/08/2022",
			},
			LinkedEntityIds: set.NewSet[string](),
		},
		{
			Id:           "d-2",
			DocumentType: "Doc-type-A",
			Attributes: map[string]string{
				"Title": "Summary 2",
				"Date":  "07/08/2022",
			},
			LinkedEntityIds: set.NewSet[string](),
		},
		{
			Id:           "d-3",
			DocumentType: "Doc-type-B",
			Attributes: map[string]string{
				"Title": "Summary 3",
				"Date":  "09/08/2022",
			},
			LinkedEntityIds: set.NewSet[string](),
		},
		{
			Id:           "d-4",
			DocumentType: "Doc-type-B",
			Attributes: map[string]string{
				"Title": "Summary 4",
				"Date":  "10/08/2022",
			},
			LinkedEntityIds: set.NewSet[string](),
		},
	}

	entities := []graphstore.Entity{
		{
			Id:         "e-1",
			EntityType: "Person",
			Attributes: map[string]string{
				"Full Name": "Bob Smith",
			},
			LinkedDocumentIds: set.NewSet[string](),
		},
		{
			Id:         "e-2",
			EntityType: "Person",
			Attributes: map[string]string{
				"Full Name": "Sally Jones",
			},
			LinkedDocumentIds: set.NewSet[string](),
		},
		{
			Id:         "e-3",
			EntityType: "Person",
			Attributes: map[string]string{
				"Full Name": "Sandra Jackson",
			},
			LinkedDocumentIds: set.NewSet[string](),
		},
		{
			Id:         "e-4",
			EntityType: "Person",
			Attributes: map[string]string{
				"Full Name": "Samuel Taylor",
			},
			LinkedDocumentIds: set.NewSet[string](),
		},
	}

	links := []graphstore.Link{
		{
			EntityId:   "e-1",
			DocumentId: "d-1",
		},
		{
			EntityId:   "e-1",
			DocumentId: "d-2",
		},
		{
			EntityId:   "e-2",
			DocumentId: "d-1",
		},
		{
			EntityId:   "e-2",
			DocumentId: "d-2",
		},
		{
			EntityId:   "e-1",
			DocumentId: "d-3",
		},
		{
			EntityId:   "e-3",
			DocumentId: "d-3",
		},
		{
			EntityId:   "e-3",
			DocumentId: "d-4",
		},
		{
			EntityId:   "e-4",
			DocumentId: "d-4",
		},
	}

	// Load the bipartite graph store with the expected contents
	assert.NoError(t, graphstore.BulkLoadBipartiteGraphStore(expectedBipartite,
		entities, documents, links))

	// Check the bipartite graph store
	assert.True(t, expectedBipartite.Equal(graphBuilder.Bipartite))

	// Check the unipartite graph
	expectedUnipartite := graphstore.NewInMemoryUnipartiteGraphStore()
	expectedUnipartite.AddUndirected("e-1", "e-2")
	expectedUnipartite.AddUndirected("e-1", "e-3")
	expectedUnipartite.AddUndirected("e-3", "e-4")

	equal, err := graphstore.UnipartiteGraphStoresEqual(expectedUnipartite, graphBuilder.Unipartite)
	assert.NoError(t, err)
	assert.True(t, equal)
}
