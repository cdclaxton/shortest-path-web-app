package graphbuilder

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/filedetector"
	"github.com/cdclaxton/shortest-path-web-app/graphloader"
	"github.com/stretchr/testify/assert"
)

func TestIsGraphBuildingRequired(t *testing.T) {

	testCases := []struct {
		description              string    // Description of the test case
		bipartiteType            string    // Type of bipartite store
		unipartiteType           string    // Type of unipartite store
		generateSignature        bool      // Generate a signature file for the previous files?
		previousFiles            GraphData // Previous files from which to build a signature
		currentFiles             GraphData // Current files to process
		expectedBuildingRequired bool      // Is graph building required?
		hasFileSignatures        bool      // File signatures should be returned?
	}{
		{
			description:              "in-memory unipartite, in-memory bipartite",
			bipartiteType:            StorageTypeInMemory,
			unipartiteType:           StorageTypeInMemory,
			generateSignature:        false,
			expectedBuildingRequired: true,
			hasFileSignatures:        false,
		},
		{
			description:              "pebble unipartite, in-memory bipartite",
			bipartiteType:            StorageTypeInMemory,
			unipartiteType:           StorageTypePebble,
			generateSignature:        false,
			expectedBuildingRequired: true,
			hasFileSignatures:        false,
		},
		{
			description:              "in-memory unipartite, pebble bipartite",
			bipartiteType:            StorageTypePebble,
			unipartiteType:           StorageTypeInMemory,
			generateSignature:        false,
			expectedBuildingRequired: true,
			hasFileSignatures:        false,
		},
		{
			description:       "pebble unipartite, pebble bipartite, no signature",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: false,
			previousFiles:     GraphData{},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
				},
				DocumentsFiles:   []graphloader.DocumentsCsvFile{},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			expectedBuildingRequired: true,
			hasFileSignatures:        true,
		},
		{
			description:       "pebble unipartite, pebble bipartite, empty signature",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: true,
			previousFiles: GraphData{
				EntitiesFiles:    []graphloader.EntitiesCsvFile{},
				DocumentsFiles:   []graphloader.DocumentsCsvFile{},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
				},
				DocumentsFiles:   []graphloader.DocumentsCsvFile{},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			expectedBuildingRequired: true,
			hasFileSignatures:        true,
		},
		{
			description:       "pebble unipartite, pebble bipartite, same file",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: true,
			previousFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
				},
				DocumentsFiles:   []graphloader.DocumentsCsvFile{},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
				},
				DocumentsFiles:   []graphloader.DocumentsCsvFile{},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			expectedBuildingRequired: false,
			hasFileSignatures:        false,
		},
		{
			description:       "pebble unipartite, pebble bipartite, different file",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: true,
			previousFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
				},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-1/data/documents-A.csv",
					},
				},
				LinksFiles:       []graphloader.LinksCsvFile{},
				SkipEntitiesFile: "",
			},
			expectedBuildingRequired: true,
			hasFileSignatures:        true,
		},
		{
			description:       "pebble unipartite, pebble bipartite, multiple files, same",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: true,
			previousFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/entities_1.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/documents_1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "../test-data-sets/set-0/data/links_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/links_1.csv",
					},
				},
				SkipEntitiesFile: "../test-data-sets/set-0/data/skip_entities.txt",
			},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/entities_1.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/documents_1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "../test-data-sets/set-0/data/links_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/links_1.csv",
					},
				},
				SkipEntitiesFile: "../test-data-sets/set-0/data/skip_entities.txt",
			},
			expectedBuildingRequired: false,
			hasFileSignatures:        false,
		},
		{
			description:       "pebble unipartite, pebble bipartite, multiple files, one different",
			bipartiteType:     StorageTypePebble,
			unipartiteType:    StorageTypePebble,
			generateSignature: true,
			previousFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/entities_1.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/documents_1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "../test-data-sets/set-0/data/links_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/links_1.csv",
					},
				},
				SkipEntitiesFile: "../test-data-sets/set-0/data/skip_entities.txt",
			},
			currentFiles: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "../test-data-sets/set-0/data/entities_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/entities_1.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "../test-data-sets/set-0/data/documents_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/documents_1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "../test-data-sets/set-0/data/links_0.csv",
					},
					{
						Path: "../test-data-sets/set-0/data/links_1.csv",
					},
				},
				SkipEntitiesFile: "",
			},
			expectedBuildingRequired: true,
			hasFileSignatures:        true,
		},
	}

	// Create a temp folder to write the signature file to
	signatureFolder, err := ioutil.TempDir("", "fileSignature")
	assert.NoError(t, err)
	defer os.RemoveAll(signatureFolder)
	signatureFile := path.Join(signatureFolder, "signature.json")

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			// Construct the graph config for the previous files if required
			var previousConfig GraphConfig
			if testCase.generateSignature {

				// Force the previous config to use a Pebble backend so that the signature
				// is returned
				previousConfig = GraphConfig{
					Data: testCase.previousFiles,
					BipartiteConfig: BipartiteGraphConfig{
						Type: StorageTypePebble,
					},
					UnipartiteConfig: UnipartiteGraphConfig{
						Type: StorageTypePebble,
					},
					SignatureFile: signatureFile,
				}

				_, sigInfo, err := isGraphBuildingRequired(previousConfig)
				assert.NoError(t, err)
				assert.NotNil(t, sigInfo)

				// Write the signature file to disk
				assert.NoError(t, filedetector.WriteFileSignatures(sigInfo, signatureFile))
			}

			// Construct the graph config for the current files
			config := GraphConfig{
				Data: testCase.currentFiles,
				BipartiteConfig: BipartiteGraphConfig{
					Type: testCase.bipartiteType,
				},
				UnipartiteConfig: UnipartiteGraphConfig{
					Type: testCase.unipartiteType,
				},
				SignatureFile: signatureFile,
			}

			// Check whether graph building is required
			buildingRequired, sigInfo, err := isGraphBuildingRequired(config)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedBuildingRequired, buildingRequired)

			if testCase.hasFileSignatures {
				assert.NotNil(t, sigInfo)
			} else {
				assert.Nil(t, sigInfo)
			}

			// Delete the signature file
			if testCase.generateSignature {
				assert.NoError(t, os.Remove(signatureFile))
			}

		})
	}
}

func TestFilesToCheck(t *testing.T) {

	testCases := []struct {
		description string
		data        GraphData
		expected    []string
	}{
		{
			description: "with skip entities",
			data: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "entity-1.csv",
					},
					{
						Path: "entity-2.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "document-1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "links-1.csv",
					},
					{
						Path: "links-2.csv",
					},
					{
						Path: "links-3.csv",
					},
				},
				SkipEntitiesFile: "skip.txt",
			},
			expected: []string{
				"entity-1.csv",
				"entity-2.csv",
				"document-1.csv",
				"links-1.csv",
				"links-2.csv",
				"links-3.csv",
				"skip.txt",
			},
		},
		{
			description: "without skip entities",
			data: GraphData{
				EntitiesFiles: []graphloader.EntitiesCsvFile{
					{
						Path: "entity-1.csv",
					},
					{
						Path: "entity-2.csv",
					},
				},
				DocumentsFiles: []graphloader.DocumentsCsvFile{
					{
						Path: "document-1.csv",
					},
				},
				LinksFiles: []graphloader.LinksCsvFile{
					{
						Path: "links-1.csv",
					},
					{
						Path: "links-2.csv",
					},
					{
						Path: "links-3.csv",
					},
				},
				SkipEntitiesFile: "",
			},
			expected: []string{
				"entity-1.csv",
				"entity-2.csv",
				"document-1.csv",
				"links-1.csv",
				"links-2.csv",
				"links-3.csv",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			actual := filesToCheck(testCase.data)
			assert.Equal(t, testCase.expected, actual)
		})
	}

}
