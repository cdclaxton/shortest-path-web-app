package server

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cdclaxton/shortest-path-web-app/spider"
	"github.com/stretchr/testify/assert"
)

func TestNewSpiderJobRunner(t *testing.T) {

	spider := spider.Spider{}
	chartBuilder := i2chart.SpiderChartBuilder{}

	// Make a folder
	folder, err := os.MkdirTemp("", "test-job-runner")
	assert.NoError(t, err)
	defer os.RemoveAll(folder) // Delete the temporary folder

	// Make a folder path that doesn't exist
	nonExistentTempFolder := folder + "-A"

	// Spider job runner with a nil Spider engine
	runner, err := NewSpiderJobRunner(nil, &chartBuilder, folder)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Spider job runner with a nil chart builder engine
	runner, err = NewSpiderJobRunner(&spider, nil, folder)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Spider job runner with a folder that doesn't exist
	runner, err = NewSpiderJobRunner(&spider, &chartBuilder, nonExistentTempFolder)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Spider job runner
	runner, err = NewSpiderJobRunner(&spider, &chartBuilder, folder)
	assert.NoError(t, err)
	assert.NotNil(t, runner)
}

// makeSpiderJobRunner for testing purposes.
func makeSpiderJobRunner(t *testing.T) *SpiderJobRunner {

	folder := "../test-data-sets/set-1/"
	dataConfigFilepath := path.Join(folder, "data-config.json")
	i2ConfigFilepath := path.Join(folder, "i2-spider-config.json")

	// Build and load the graphs
	builder, err := graphbuilder.NewGraphBuilderFromJson(dataConfigFilepath)
	assert.NoError(t, err)

	// Instantiate the i2 chart builder for spidering
	chartBuilder, err := i2chart.NewSpiderChartBuilder(i2ConfigFilepath)
	assert.NoError(t, err)
	chartBuilder.SetBipartite(builder.Bipartite)

	// Instantiate the spider engine
	spider, err := spider.NewSpider(builder.Unipartite)
	assert.NoError(t, err)

	// Make a temporary folder for the output Excel files
	tempFolder, err := os.MkdirTemp("", "test-job-runner")
	assert.NoError(t, err)

	// Check that the output folder has been made
	_, err = os.Stat(tempFolder)
	assert.False(t, os.IsNotExist(err))

	// Make the spider job runner
	runner, err := NewSpiderJobRunner(spider, chartBuilder, tempFolder)
	assert.NoError(t, err)

	return runner
}

func cleanUpSpiderJobRunner(t *testing.T, runner *SpiderJobRunner) {
	assert.NoError(t, os.RemoveAll(runner.folder))
}

func waitForSpiderJobsToFinish(runner *SpiderJobRunner) {
	for {
		if runner.GetNumberJobsExecuting() == 0 {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func checkSpiderJob(t *testing.T, spiderJob *job.SpiderJob, expectedGUID string,
	expectedConfiguration *job.SpiderJobConfiguration,
	expectedJobState job.JobState, shouldHaveResultsFile bool,
	expectedMessage string, shouldHaveError bool) {

	// Check the job is not nil
	assert.NotNil(t, spiderJob)

	// Check the GUID
	assert.Equal(t, expectedGUID, spiderJob.GUID)

	// Check the job configuration
	assert.Equal(t, expectedConfiguration, spiderJob.Configuration)

	// Check the job state
	assert.Equal(t, expectedJobState, spiderJob.Progress.State)

	// All jobs should have a start and end time, even if they failed
	assert.False(t, spiderJob.Progress.StartTime.IsZero())
	assert.False(t, spiderJob.Progress.EndTime.IsZero())
	assert.False(t, spiderJob.Progress.EndTime.Before(spiderJob.Progress.StartTime))

	// Check whether there is a results file to download
	if shouldHaveResultsFile {
		assert.True(t, len(spiderJob.ResultFile) > 0)
	} else {
		assert.Equal(t, "", spiderJob.ResultFile)
	}

	// Check the message
	assert.Equal(t, expectedMessage, spiderJob.Message)

	// Check the error
	if shouldHaveError {
		assert.NotNil(t, spiderJob.Error)
	} else {
		assert.Nil(t, spiderJob.Error)
	}
}

func TestSpiderJobRunner(t *testing.T) {
	spiderJobRunner := makeSpiderJobRunner(t)
	defer cleanUpSpiderJobRunner(t, spiderJobRunner)

	// Try to get a job that doesn't exist
	j, err := spiderJobRunner.GetJob("1234")
	assert.Error(t, err)
	assert.Nil(t, j)

	// Try to check if a job is finished that doesn't exist
	finished, err := spiderJobRunner.IsJobFinished("1234")
	assert.Error(t, err)
	assert.False(t, finished)

	// Try to submit a job that has invalid configuration
	conf := &job.SpiderJobConfiguration{}
	guid, err := spiderJobRunner.Submit(conf)
	assert.Error(t, err)
	assert.Equal(t, InvalidGUID, guid)

	// Run a job that will return no connections
	conf, err = job.NewSpiderJobConfiguration(0, set.NewPopulatedSet("e-1"))
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	guid, err = spiderJobRunner.Submit(conf)
	assert.NoError(t, err)
	assert.Equal(t, 36, len(guid))

	waitForSpiderJobsToFinish(spiderJobRunner)

	j1, err := spiderJobRunner.GetJob(guid)
	assert.NoError(t, err)
	assert.NotNil(t, j1)

	checkSpiderJob(t, j1, guid, conf, job.CompleteNoResults, false,
		noPathsMessageFromSpidering, false)

	// Check that the job is finished
	finished, err = spiderJobRunner.IsJobFinished(guid)
	assert.NoError(t, err)
	assert.True(t, finished)

	// Run a job that will return an i2 chart
	conf, err = job.NewSpiderJobConfiguration(1, set.NewPopulatedSet("e-1"))
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	guid, err = spiderJobRunner.Submit(conf)
	assert.NoError(t, err)
	assert.Equal(t, 36, len(guid))

	waitForSpiderJobsToFinish(spiderJobRunner)

	j1, err = spiderJobRunner.GetJob(guid)
	assert.NoError(t, err)
	assert.NotNil(t, j1)

	checkSpiderJob(t, j1, guid, conf, job.CompleteResults, true, "", false)

	// Check the data written to the generated Excel file
	expectedTable := [][]string{
		{
			"ID-1",
			"Type-1",
			"Icon-1",
			"Label-1",
			"Seed-1",
			"ID-2",
			"Type-2",
			"Icon-2",
			"Label-2",
			"Seed-2",
		},
		{
			"e-1",
			"Person",
			"Anonymous",
			"Bob Smith",
			"TRUE",
			"e-2",
			"Person",
			"Anonymous",
			"Sally Jones",
			"FALSE",
		},
		{
			"e-1",
			"Person",
			"Anonymous",
			"Bob Smith",
			"TRUE",
			"e-3",
			"Address",
			"Location",
			"31 Field Drive, EH36 5PB",
			"FALSE",
		},
	}
	actualTable, err := i2chart.ReadFromExcel(j1.ResultFile, "Sheet1")
	assert.NoError(t, err)
	assert.Equal(t, expectedTable, actualTable)
}
