package graphbuilder

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/loader"
	"github.com/rs/zerolog/log"
)

// GraphData specifies the location of the input data to read.
type GraphData struct {
	EntitiesFiles    []loader.EntitiesCsvFile  `json:"entitiesFiles"`
	DocumentsFiles   []loader.DocumentsCsvFile `json:"documentsFiles"`
	LinksFiles       []loader.LinksCsvFile     `json:"linksFiles"`
	SkipEntitiesFile string                    `json:"skipEntitiesFile"` // File path to the entities to skip
}

// readGraphDataConfig from a JSON file.
func readGraphDataConfig(filepath string) (*GraphData, error) {

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
	graphData := GraphData{}
	err = json.Unmarshal(content, &graphData)

	if err != nil {
		return nil, err
	}

	return &graphData, nil
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
	Type string
}

// UnipartiteGraphConfig to instantiate a unipartite graph store.
type UnipartiteGraphConfig struct {
	Type string
}

// GraphConfig for the input data, bipartite and unipartite graphs.
type GraphConfig struct {
	Data             GraphData
	BipartiteConfig  BipartiteGraphConfig
	UnipartiteConfig UnipartiteGraphConfig
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
