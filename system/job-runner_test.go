package system

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/stretchr/testify/assert"
)

func TestMakeExcelFilepath(t *testing.T) {
	result := makeExcelFilepath("../data/output", "1234")
	assert.Equal(t, "../data/output/1234.xlsx", result)
}

// makeJobRunner for testing purposes that is configured to be able to run
// jobs successfully.
func makeJobRunner(t *testing.T) *JobRunner {

	folder := "../test-data-sets/set-1/"
	dataConfigFilepath := path.Join(folder, "data-config.json")
	i2ConfigFilepath := path.Join(folder, "i2-config.json")

	// Build and load the graphs
	builder, err := graphbuilder.NewGraphBuilderFromJson(dataConfigFilepath)
	assert.NoError(t, err)

	// Instantiate the i2 chart builder
	chartBuilder, err := i2chart.NewI2ChartBuilder(i2ConfigFilepath)
	assert.NoError(t, err)
	chartBuilder.SetBipartite(builder.Bipartite)

	// Instantiate the path finder
	pathFinder, err := bfs.NewPathFinder(builder.Unipartite)
	assert.NoError(t, err)

	// Make a temporary folder for the output Excel files
	tempFolder, err := ioutil.TempDir("", "test-job-runner")
	assert.NoError(t, err)

	// Make the job runner
	runner, err := NewJobRunner(pathFinder, chartBuilder, tempFolder)
	assert.NoError(t, err)

	return runner
}

func cleanUpJobRunner(t *testing.T, runner *JobRunner) {
	assert.NoError(t, os.RemoveAll(runner.folder))
}

func TestSubmitJob(t *testing.T) {
	runner := makeJobRunner(t)
	defer cleanUpJobRunner(t, runner)

	// Try to get a job that doesn't exist
	j, err := runner.GetJob("1234")
	assert.Error(t, err)
	assert.Nil(t, j)

	// Try to submit a job that has invalid configuration
	conf := job.JobConfiguration{}
	guid, err := runner.Submit(&conf)
	assert.Error(t, err)
	assert.Equal(t, InvalidGUID, guid)

}
