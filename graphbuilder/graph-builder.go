package graphbuilder

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/loader"
	"github.com/rs/zerolog/log"
)

const DataDirectory = "data"

// GraphData specifies the location of the input data to read.
type GraphData struct {
	EntitiesFiles    []loader.EntitiesCsvFile  `json:"entitiesFiles"`
	DocumentsFiles   []loader.DocumentsCsvFile `json:"documentsFiles"`
	LinksFiles       []loader.LinksCsvFile     `json:"linksFiles"`
	SkipEntitiesFile string                    `json:"skipEntitiesFile"` // File path to the entities to skip
}

// makeBipartiteGraph given the bipartite graph storage config.
func makeBipartiteGraph(config BipartiteGraphConfig) (graphstore.BipartiteGraphStore, error) {
	if config.Type == "memory" {
		return graphstore.NewInMemoryBipartiteGraphStore(), nil
	}

	return nil, fmt.Errorf("Unknown bipartite graph storage type: %v", config.Type)
}

// makeUnipartiteGraph given the unipartite graph storage config.
func makeUnipartiteGraph(config UnipartiteGraphConfig) (graphstore.UnipartiteGraphStore, error) {
	if config.Type == "memory" {
		return graphstore.NewInMemoryUnipartiteGraphStore(), nil
	}

	return nil, fmt.Errorf("Unknown unipartite graph storage type: %v", config.Type)
}

// BipartiteGraphConfig to instantiate a bipartite graph store.
type BipartiteGraphConfig struct {
	Type   string `json:"type"`   // Backend type (in-memory or Pebble)
	Folder string `json:"folder"` // Folder for the Pebble store
}

// UnipartiteGraphConfig to instantiate a unipartite graph store.
type UnipartiteGraphConfig struct {
	Type   string `json:"type"`   // Backend type (in-memory or Pebble)
	Folder string `json:"folder"` // Folder for the Pebble store
}

// GraphConfig for the input data, bipartite and unipartite graphs.
type GraphConfig struct {
	Data             GraphData             `json:"graphData"`
	BipartiteConfig  BipartiteGraphConfig  `json:"bipartiteGraphConfig"`
	UnipartiteConfig UnipartiteGraphConfig `json:"unipartiteGraphConfig"`
}

// readGraphConfig from a JSON file.
func readGraphConfig(filepath string) (*GraphConfig, error) {

	// Check the file exists
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Read the JSON into a byte array
	content, err := ioutil.ReadAll(file)
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
	log.Info().Str("Component", "GraphBuilder").Msg("Making the bipartite graph store from config")
	var err error
	builder.Bipartite, err = makeBipartiteGraph(config.BipartiteConfig)
	if err != nil {
		return nil, err
	}

	// Load the bipartite graph based on the files
	log.Info().Str("Component", "GraphBuilder").
		Msg("Loading the bipartite graph store from CSV files")

	bipartiteLoader := loader.NewGraphStoreLoaderFromCsv(builder.Bipartite,
		config.Data.EntitiesFiles,
		config.Data.DocumentsFiles,
		config.Data.LinksFiles)

	err = bipartiteLoader.Load()
	if err != nil {
		return nil, err
	}

	// Read the entities to skip
	log.Info().Str("Component", "GraphBuilder").Msg("Reading the entities to skip")
	skipEntities, err := loader.ReadSkipEntities(config.Data.SkipEntitiesFile)
	if err != nil {
		return nil, err
	}

	// Make the unipartite graph store
	log.Info().Str("Component", "GraphBuilder").Msg("Making the unipartite graph store")
	builder.Unipartite, err = makeUnipartiteGraph(config.UnipartiteConfig)
	if err != nil {
		return nil, err
	}

	// Convert the bipartite graph to a unipartite graph
	log.Info().Str("Component", "GraphBuilder").
		Msg("Converting the bipartite graph to a unipartite graph")
	graphstore.BipartiteToUnipartite(builder.Bipartite, builder.Unipartite, skipEntities)

	return &builder, nil
}

func NewGraphBuilderFromJson(filepath string) (*GraphBuilder, error) {

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
func (gb *GraphBuilder) Destory() error {
	err := gb.Unipartite.Destroy()
	if err != nil {
		return err
	}

	return gb.Bipartite.Destroy()
}
