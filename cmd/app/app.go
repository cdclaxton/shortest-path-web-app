package main

import (
	"flag"
	"io"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/search"
	"github.com/cdclaxton/shortest-path-web-app/server"
)

// Component name used in logging
const componentName = "application"

// readMessage from a file that gets displayed on the index page.
func readMessage(filepath string) (string, error) {

	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func main() {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting shortest path web-app")

	// Get the config path and the i2 config path
	dataConfigPath := flag.String("data", "data-config.json", "Path to the config.json file")
	i2ConfigPath := flag.String("i2", "i2-config.json", "Path to the i2 config.json file")
	chartFolder := flag.String("folder", "./chartFolder", "Folder for storing generated charts")
	messagePath := flag.String("message", "message.html", "Path to message to show on index page")

	flag.Parse()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", *dataConfigPath).
		Msg("Data config filepath")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", *i2ConfigPath).
		Msg("i2 config filepath")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", *chartFolder).
		Msg("i2 chart folder")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", *messagePath).
		Msg("index page message path")

	// Load the message
	msg, err := readMessage(*messagePath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to read message file")
	}

	// Create the bipartite and unipartite graphs
	builder, err := graphbuilder.NewGraphBuilderFromJson(*dataConfigPath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create graph builder")
	}

	// Create the i2 chart builder
	chartBuilder, err := i2chart.NewI2ChartBuilder(*i2ConfigPath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create chart builder")
	}

	// Set the bipartite graph in the i2 chart builder
	chartBuilder.SetBipartite(builder.Bipartite)

	// Instantiate the path finder
	pathFinder, err := bfs.NewPathFinder(builder.Unipartite)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create path finder")
	}

	// Create the search engine
	searchEngine, err := search.NewEntitySearch(builder.Bipartite, builder.Unipartite)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create search engine")
	}

	// Create the job runner
	runner, err := server.NewJobRunner(pathFinder, chartBuilder, *chartFolder, searchEngine)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create job runner")
	}

	// Create the job server
	jobServer, err := server.NewJobServer(runner, msg, builder.Stats)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create job server")
	}

	// Start the job server (ready for users to run jobs)
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting server")

	jobServer.Start()
}
