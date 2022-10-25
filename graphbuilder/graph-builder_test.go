package graphbuilder

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphloader"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
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
	filepath := "../test-data-sets/set-0/config-inmemory.json"
	graphConfig, err := readGraphConfig(filepath)
	assert.NoError(t, err)

	// Check the entity files
	expectedEntityFiles := []graphloader.EntitiesCsvFile{
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
	expectedDocumentsFiles := []graphloader.DocumentsCsvFile{
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
	expectedLinksFiles := []graphloader.LinksCsvFile{
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
			EntitiesFiles: []graphloader.EntitiesCsvFile{
				{
					Path: "entities_1.csv",
				},
			},
			DocumentsFiles: []graphloader.DocumentsCsvFile{
				{
					Path: "documents_1.csv",
				},
			},
			LinksFiles: []graphloader.LinksCsvFile{
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

// buildExpectedBipartiteStore for sets 0 and 2
func buildExpectedBipartiteStore(t *testing.T) graphstore.BipartiteGraphStore {

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

	return expectedBipartite
}

func buildExpectedUnipartiteStore(t *testing.T) graphstore.UnipartiteGraphStore {
	expectedUnipartite := graphstore.NewInMemoryUnipartiteGraphStore()
	expectedUnipartite.AddUndirected("e-1", "e-2")
	expectedUnipartite.AddUndirected("e-1", "e-3")
	expectedUnipartite.AddUndirected("e-3", "e-4")

	return expectedUnipartite
}

// Test the graph builder with valid config. Each config points to the
func TestGraphBuilderValidConfig(t *testing.T) {

	testCases := []struct {
		configFilepath string
	}{
		{
			// In-memory
			configFilepath: "../test-data-sets/set-0/config-inmemory.json",
		},
		{
			// Pebble
			configFilepath: "../test-data-sets/set-0/config-pebble.json",
		},
	}

	for _, testCase := range testCases {

		// Instantiate the graph builder
		graphBuilder, err := NewGraphBuilderFromJson(testCase.configFilepath)
		assert.NoError(t, err)

		// Get the expected bipartite graph store
		expectedBipartite := buildExpectedBipartiteStore(t)

		// Check the bipartite graph store
		eq, err := expectedBipartite.Equal(graphBuilder.Bipartite)
		assert.NoError(t, err)
		assert.True(t, eq)

		// Get the expected unipartite graph
		expectedUnipartite := buildExpectedUnipartiteStore(t)

		// Check the unipartite graph
		equal, err := graphstore.UnipartiteGraphStoresEqual(expectedUnipartite, graphBuilder.Unipartite)
		assert.NoError(t, err)
		assert.True(t, equal)

		// Destroy the graph databases
		graphBuilder.Destroy()
	}

}

func TestPrepareFolderForStorage(t *testing.T) {

	// Create a temporary folder
	tempFolder, err := ioutil.TempDir("", "folder-test")
	assert.NoError(t, err)

	// Prepare an empty folder
	assert.NoError(t, prepareFolderForStorage(tempFolder, "test", false))

	// Create a file within the folder
	file, err := os.Create(filepath.Join(tempFolder, "test.txt"))
	assert.NoError(t, err)
	file.Close()

	// Try to prepare the folder. It should fail because the folder isn't empty and permission is
	// not given to clear down the folder.
	assert.Error(t, prepareFolderForStorage(tempFolder, "test", false))

	// Try preparing the folder with permission to clear it down
	assert.NoError(t, prepareFolderForStorage(tempFolder, "test", true))

	// Delete the temp folder
	assert.NoError(t, os.RemoveAll(tempFolder))
}
