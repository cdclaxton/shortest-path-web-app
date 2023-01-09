package server

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/search"
	"github.com/stretchr/testify/assert"
)

// makeJobRunner for testing purposes that is configured to be able to run
// jobs successfully. Ensure cleanUpJobRunner() is called after testing is complete.
func makeJobRunner(t *testing.T) *JobRunner {

	folder := "../test-data-sets/set-1/"
	dataConfigFilepath := path.Join(folder, "data-config.json")
	i2ConfigFilepath := path.Join(folder, "i2-config.json")

	// Build and load the graphs
	builder, err := graphbuilder.NewGraphBuilderFromJson(dataConfigFilepath)
	assert.NoError(t, err)

	// Entity search engine
	searchEngine, err := search.NewEntitySearch(builder.Bipartite, builder.Unipartite)

	// Instantiate the i2 chart builder
	chartBuilder, err := i2chart.NewI2ChartBuilder(i2ConfigFilepath)
	assert.NoError(t, err)
	chartBuilder.SetBipartite(builder.Bipartite)

	// Instantiate the path finder
	pathFinder, err := bfs.NewPathFinder(builder.Unipartite)
	assert.NoError(t, err)

	// Make a temporary folder for the output Excel files
	tempFolder, err := os.MkdirTemp("", "test-job-runner")
	assert.NoError(t, err)

	// Check that the output folder has been made
	_, err = os.Stat(tempFolder)
	assert.False(t, os.IsNotExist(err))

	// Make the job runner
	runner, err := NewJobRunner(pathFinder, chartBuilder, tempFolder, searchEngine)
	assert.NoError(t, err)

	return runner
}

// cleanUpJobRunner removes the runner folder.
func cleanUpJobRunner(t *testing.T, runner *JobRunner) {
	assert.NoError(t, os.RemoveAll(runner.folder))
}

// waitForJobsToFinish so that testing can continue with completed jobs.
func waitForJobsToFinish(runner *JobRunner) {

	for {
		if runner.GetNumberJobsExecuting() == 0 {
			return
		}
		time.Sleep(1 * time.Second)
	}
}
