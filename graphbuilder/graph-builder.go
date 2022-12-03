package graphbuilder

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cdclaxton/shortest-path-web-app/graphloader"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

const componentName = "graphBuilder"

const (
	DataDirectory            = "data"              // Location for the input entities and document files
	StorageTypeInMemory      = "memory"            // In-memory storage
	StorageTypePebble        = "pebble"            // Pebble storage
	UseTempFolder            = "<TEMP>"            // Denotes that a temporary folder should be made for Pebble files
	TempBipartiteFolderName  = "pebble-bipartite"  // Temporary folder name (prefix) for the bipartite store
	TempUnipartiteFolderName = "pebble-unipartite" // Temporary folder name (prefix) for the unipartite store
)

// GraphData specifies the location of the input data to read.
type GraphData struct {
	EntitiesFiles    []graphloader.EntitiesCsvFile  `json:"entitiesFiles"`
	DocumentsFiles   []graphloader.DocumentsCsvFile `json:"documentsFiles"`
	LinksFiles       []graphloader.LinksCsvFile     `json:"linksFiles"`
	SkipEntitiesFile string                         `json:"skipEntitiesFile"` // File path to the entities to skip
}

// createTempBipartitePebbleFolder in the default temp directory for the operating system.
func createTempBipartitePebbleFolder() (string, error) {
	return os.MkdirTemp("", TempBipartiteFolderName)
}

// createTempUnipartitePebbleFolder in the default temp directory for the operating system.
func createTempUnipartitePebbleFolder() (string, error) {
	return os.MkdirTemp("", TempUnipartiteFolderName)
}

// prepareFolderForStorage by ensuring it is empty.
func prepareFolderForStorage(folder string, graphStoreType string, deleteFilesInFolder bool) error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", folder).
		Str("graphStoreType", graphStoreType).
		Msg("Preparing folder for Pebble storage")

	// Check if the folder is empty
	folderEmpty, err := isFolderEmpty(folder)
	if err != nil {
		return err
	}

	// If the folder isn't empty, then clear it if config allows
	if !folderEmpty {
		if !deleteFilesInFolder {
			return fmt.Errorf("folder for %v graph store (%v) isn't empty", graphStoreType, folder)
		} else {
			err := clearFolder(folder)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// makeBipartiteGraph given the bipartite graph storage config.
func makeBipartiteGraph(config BipartiteGraphConfig) (graphstore.BipartiteGraphStore, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("graphStoreType", config.Type).
		Msg("Making bipartite graph store")

	if config.Type == StorageTypeInMemory {
		return graphstore.NewInMemoryBipartiteGraphStore(), nil

	} else if config.Type == StorageTypePebble {

		// If the config specifies that a temporary folder should be used, then make the folder
		if config.Folder == UseTempFolder {
			tempFolder, err := createTempBipartitePebbleFolder()
			if err != nil {
				return nil, err
			}
			config.Folder = tempFolder

			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("tempFolder", tempFolder).
				Msg("Made temp folder for the bipartite Pebble-backed graph")
		}

		// Prepare the folder
		err := prepareFolderForStorage(config.Folder, "bipartite", config.DeleteFilesInFolder)
		if err != nil {
			return nil, err
		}

		return graphstore.NewPebbleBipartiteGraphStore(config.Folder)
	}

	return nil, fmt.Errorf("unknown bipartite graph storage type: %v", config.Type)
}

// makeUnipartiteGraph given the unipartite graph storage config.
func makeUnipartiteGraph(config UnipartiteGraphConfig) (graphstore.UnipartiteGraphStore, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("graphStoreType", config.Type).
		Msg("Making unipartite graph store")

	if config.Type == StorageTypeInMemory {
		return graphstore.NewInMemoryUnipartiteGraphStore(), nil

	} else if config.Type == StorageTypePebble {

		// If the config specifies that a temporary folder should be used, then make the folder
		if config.Folder == UseTempFolder {
			tempFolder, err := createTempUnipartitePebbleFolder()
			if err != nil {
				return nil, err
			}
			config.Folder = tempFolder

			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("tempFolder", tempFolder).
				Msg("Made temp folder for the unipartite Pebble-backed graph")
		}

		// Prepare the folder
		err := prepareFolderForStorage(config.Folder, "unipartite", config.DeleteFilesInFolder)
		if err != nil {
			return nil, err
		}

		return graphstore.NewPebbleUnipartiteGraphStore(config.Folder)
	}

	return nil, fmt.Errorf("unknown unipartite graph storage type: %v", config.Type)
}

// BipartiteGraphConfig to instantiate a bipartite graph store.
type BipartiteGraphConfig struct {
	Type                string `json:"type"`                // Backend type (in-memory or Pebble)
	Folder              string `json:"folder"`              // Folder for the Pebble store
	DeleteFilesInFolder bool   `json:"deleteFilesInFolder"` // Clear down the folder if it isn't empty
}

// UnipartiteGraphConfig to instantiate a unipartite graph store.
type UnipartiteGraphConfig struct {
	Type                string `json:"type"`                // Backend type (in-memory or Pebble)
	Folder              string `json:"folder"`              // Folder for the Pebble store
	DeleteFilesInFolder bool   `json:"deleteFilesInFolder"` // Clear down the folder if it isn't empty
}

// GraphConfig for the input data, bipartite and unipartite graphs.
type GraphConfig struct {
	Data               GraphData             `json:"graphData"`
	BipartiteConfig    BipartiteGraphConfig  `json:"bipartiteGraphConfig"`
	UnipartiteConfig   UnipartiteGraphConfig `json:"unipartiteGraphConfig"`
	IgnoreInvalidLinks bool                  `json:"ignoreInvalidLinks"`
}

// readGraphConfig from a JSON file.
func readGraphConfig(filepath string) (*GraphConfig, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", filepath).
		Msg("Reading graph config from JSON file")

	// Check the file exists
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Read the JSON into a byte array
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Unmarshall the data
	graphConfig := GraphConfig{}
	err = json.Unmarshal(content, &graphConfig)

	if err != nil {
		return nil, err
	}

	return &graphConfig, nil
}

// makePathRelative
func makePathRelative(dataFilename string, configFilepath string) string {

	// Directory containing the config file
	dir := filepath.Dir(configFilepath)

	return filepath.Join(dir, DataDirectory, dataFilename)
}

// makePathsRelativeToConfig file location. It is assumed that the paths of the files to read
// are relative to the location of the config file if the paths are relative paths.
func makePathsRelativeToConfig(configFilepath string, graphConfig *GraphConfig) {

	// Entities
	for idx, entitiesFile := range graphConfig.Data.EntitiesFiles {
		graphConfig.Data.EntitiesFiles[idx].Path = makePathRelative(
			entitiesFile.Path, configFilepath)
	}

	// Documents
	for idx, documentsFile := range graphConfig.Data.DocumentsFiles {
		graphConfig.Data.DocumentsFiles[idx].Path = makePathRelative(
			documentsFile.Path, configFilepath)
	}

	// Links
	for idx, linksFile := range graphConfig.Data.LinksFiles {
		graphConfig.Data.LinksFiles[idx].Path = makePathRelative(linksFile.Path, configFilepath)
	}

	// Skip file
	graphConfig.Data.SkipEntitiesFile = makePathRelative(
		graphConfig.Data.SkipEntitiesFile, configFilepath)
}

// GraphBuilder component to build the bipartite and unipartite graphs.
type GraphBuilder struct {
	Bipartite  graphstore.BipartiteGraphStore
	Unipartite graphstore.UnipartiteGraphStore
}

func NewGraphBuilder(config GraphConfig) (*GraphBuilder, error) {

	builder := GraphBuilder{}

	// Make the bipartite graph store
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Making the bipartite graph store from config")

	var err error
	builder.Bipartite, err = makeBipartiteGraph(config.BipartiteConfig)
	if err != nil {
		return nil, err
	}

	// Load the bipartite graph based on the files
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Loading the bipartite graph store from CSV files")

	bipartiteLoader := graphloader.NewGraphStoreLoaderFromCsv(
		builder.Bipartite,
		config.Data.EntitiesFiles,
		config.Data.DocumentsFiles,
		config.Data.LinksFiles,
		config.IgnoreInvalidLinks)

	err = bipartiteLoader.Load()
	if err != nil {
		return nil, err
	}

	// Read the entities to skip
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Reading the entities to skip")

	skipEntities, err := graphloader.ReadSkipEntities(config.Data.SkipEntitiesFile)
	if err != nil {
		return nil, err
	}

	// Make the unipartite graph store
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Making the unipartite graph store")

	builder.Unipartite, err = makeUnipartiteGraph(config.UnipartiteConfig)
	if err != nil {
		return nil, err
	}

	// Convert the bipartite graph to a unipartite graph
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Converting the bipartite graph to a unipartite graph")

	graphstore.BipartiteToUnipartite(builder.Bipartite, builder.Unipartite, skipEntities)

	return &builder, nil
}

// NewGraphBuilderFromJson returns a constructed GraphBuilder based on the config from a JSON file.
func NewGraphBuilderFromJson(filepath string) (*GraphBuilder, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", filepath).
		Msg("Building graph from JSON config file")

	// Read the config from file
	graphConfig, err := readGraphConfig(filepath)
	if err != nil {
		return nil, err
	}

	// Modify the data file paths to be based on the location of the config file
	makePathsRelativeToConfig(filepath, graphConfig)

	// Instantiate the graph builder
	return NewGraphBuilder(*graphConfig)
}

// Destroy the unipartite and bipartite graphs.
func (gb *GraphBuilder) Destroy() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Destroying the unipartite and bipartite graphs")

	err := gb.Unipartite.Destroy()
	if err != nil {
		return err
	}

	return gb.Bipartite.Destroy()
}
