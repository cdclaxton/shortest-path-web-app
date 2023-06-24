package main

import (
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/search"
	"github.com/cdclaxton/shortest-path-web-app/server"
	"github.com/cdclaxton/shortest-path-web-app/spider"
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

	startTime := time.Now()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting shortest path web-app")

	// Get the config path and the i2 config path
	dataConfigPath := flag.String("data", "data-config.json", "Path to the config.json file")
	i2ConfigPath := flag.String("i2", "i2-config.json", "Path to the i2 config.json file")
	i2SpiderConfigPath := flag.String("i2spider", "i2-spider-config.json", "Path to the i2 spider config.json file")
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
		Str("filepath", *i2SpiderConfigPath).
		Msg("i2 spider config filepath")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", *chartFolder).
		Msg("i2 chart folder")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", *messagePath).
		Msg("index page message path")

	// Read the message to present on the frontend
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Reading message")
	msg, err := readMessage(*messagePath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to read message file")
	}

	// Create the bipartite and unipartite graphs
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Creating bipartite and unipartite graphs")
	builder, build, err := graphbuilder.NewGraphBuilderFromJson(*dataConfigPath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create graph builder")
	}

	logging.Logger.Info().
		Bool("buildRequired", build).
		Msg("Unipartite and bipartite graphs built")

	// Create the i2 chart builder
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making i2 chart builder")
	chartBuilder, err := i2chart.NewI2ChartBuilder(*i2ConfigPath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create chart builder")
	}

	// Create the i2 spider chart builder
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making i2 spider chart builder")
	spiderChartBuilder, err := i2chart.NewSpiderChartBuilder(*i2SpiderConfigPath)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create spider chart builder")
	}

	// Set the bipartite graph in the i2 chart builders
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Setting bipartite graph in chart builders")
	chartBuilder.SetBipartite(builder.Bipartite)
	spiderChartBuilder.SetBipartite(builder.Bipartite)

	// Instantiate the path finder
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Instantiating a path finder")
	pathFinder, err := bfs.NewPathFinder(builder.Unipartite)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create path finder")
	}

	// Instantiate the spider matcher
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Instantiating a spider matcher")
	spider, err := spider.NewSpider(builder.Unipartite)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create spider engine")
	}

	// Create the search engine
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making entity search engine")
	searchEngine, err := search.NewEntitySearch(builder.Bipartite, builder.Unipartite)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create search engine")
	}

	// Create the job runner
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making job runner")
	runner, err := server.NewJobRunner(pathFinder, chartBuilder, *chartFolder, searchEngine)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create job runner")
	}

	// Create the spider job runner
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making spider job runner")
	spiderJobRunner, err := server.NewSpiderJobRunner(spider, spiderChartBuilder, *chartFolder)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create spider job runner")
	}

	// Create the job server
	logging.Logger.Info().Str(logging.ComponentField, componentName).Msg("Making job server")
	jobServer, err := server.NewJobServer(runner, spiderJobRunner, msg, builder.Stats)
	if err != nil {
		logging.Logger.Fatal().
			Str(logging.ComponentField, componentName).
			Err(err).
			Msg("Failed to create job server")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("startUpTime", time.Since(startTime).String()).
		Msg("Start up time")

	// Start the job server (ready for users to run jobs)
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting server")

	go jobServer.Start()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	logging.Logger.Info().Msg("Running until signal")

	sig := <-stopChan
	logging.Logger.Warn().
		Str(logging.ComponentField, componentName).
		Str("signal", sig.String()).
		Msg("Shutdown signal received")

	builder.Bipartite.Close()
	builder.Unipartite.Close()
}
