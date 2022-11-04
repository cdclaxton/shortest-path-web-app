package server

import (
	"os"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/stretchr/testify/assert"
)

func TestNewJobRunner(t *testing.T) {

	// Make simple PathFinder and ChartBuilder structs
	pathFinder := bfs.PathFinder{}
	chartBuilder := &i2chart.I2ChartBuilder{}

	// Make a folder
	folder, err := os.MkdirTemp("", "test-job-runner")
	assert.NoError(t, err)
	defer os.RemoveAll(folder) // Delete the temporary folder

	// Make a folder path that doesn't exist
	nonExistentTempFolder := folder + "-A"

	// Job runner with a nil Pathfinder
	runner, err := NewJobRunner(nil, chartBuilder, folder)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Job runner with a nil Chartbuilder
	runner, err = NewJobRunner(&pathFinder, nil, folder)
	assert.Error(t, err)
	assert.Nil(t, runner)

	// Job runner with a folder that doesn't exist
	runner, err = NewJobRunner(&pathFinder, chartBuilder, nonExistentTempFolder)
	assert.Error(t, err)
	assert.Nil(t, runner)
}

func TestMakeExcelFilepath(t *testing.T) {
	result := makeExcelFilepath("../data/output", "1234")
	assert.Equal(t, "../data/output/1234.xlsx", result)
}

func checkJob(t *testing.T, j1 *job.Job,
	expectedGUID string, expectedConfiguration *job.JobConfiguration,
	expectedJobState job.JobState, shouldHaveResultsFile bool,
	expectedMessage string, shouldHaveError bool) {

	// Check the job is not nil
	assert.NotNil(t, j1)

	// Check the GUID
	assert.Equal(t, expectedGUID, j1.GUID)

	// Check the job configuration (i.e. entity sets)
	assert.Equal(t, expectedConfiguration, j1.Configuration)

	// Check the job state
	assert.Equal(t, expectedJobState, j1.Progress.State)

	// All jobs should have a start and end time, even if they failed
	assert.False(t, j1.Progress.StartTime.IsZero())
	assert.False(t, j1.Progress.EndTime.IsZero())
	assert.False(t, j1.Progress.EndTime.Before(j1.Progress.StartTime))

	// Check whether there is a results file to download
	if shouldHaveResultsFile {
		assert.True(t, len(j1.ResultFile) > 0)
	} else {
		assert.Equal(t, "", j1.ResultFile)
	}

	// Check the message
	assert.Equal(t, expectedMessage, j1.Message)

	// Check the error
	if shouldHaveError {
		assert.NotNil(t, j1.Error)
	} else {
		assert.Nil(t, j1.Error)
	}
}

func TestSubmitJob(t *testing.T) {
	runner := makeJobRunner(t)
	defer cleanUpJobRunner(t, runner)

	// Try to get a job that doesn't exist
	j, err := runner.GetJob("1234")
	assert.Error(t, err)
	assert.Nil(t, j)

	// Try to get whether a job is finished that doesn't exist
	finished, err := runner.IsJobFinished("1234")
	assert.Error(t, err)
	assert.False(t, finished)

	// Try to submit a job that has invalid configuration
	conf := &job.JobConfiguration{}
	guid, err := runner.Submit(conf)
	assert.Error(t, err)
	assert.Equal(t, InvalidGUID, guid)

	// Run a job that will return no paths
	// The graph can be found in ../test-data-sets/set-1/readme.md
	entitySets := []job.EntitySet{
		{
			Name:      "Set-1",
			EntityIds: []string{"e-1", "e-4"},
		},
	}

	conf, err = job.NewJobConfiguration(entitySets, 1)
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	guid, err = runner.Submit(conf)
	assert.NoError(t, err)
	assert.Equal(t, 36, len(guid))

	waitForJobsToFinish(runner)

	j1, err := runner.GetJob(guid)
	assert.NoError(t, err)
	assert.NotNil(t, j1)

	checkJob(t, j1, guid, conf, job.CompleteNoResults, false, noPathsMessage, false)

	// Check that the job is finished
	finished, err = runner.IsJobFinished(guid)
	assert.NoError(t, err)
	assert.True(t, finished)

	// Run a job that will return paths
	conf, err = job.NewJobConfiguration(entitySets, 2)
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	guid, err = runner.Submit(conf)
	assert.NoError(t, err)
	assert.Equal(t, 36, len(guid))

	waitForJobsToFinish(runner)

	j1, err = runner.GetJob(guid)
	assert.NoError(t, err)
	assert.NotNil(t, j1)

	checkJob(t, j1, guid, conf, job.CompleteResults, true, "", false)

	// Check the data written to the file
	expectedTable := [][]string{
		{"Entity-icon-1", "Entity-id-1", "Entity-label-1", "Entity-entitySets-1", "Entity-description-1", "Entity-icon-2", "Entity-id-2", "Entity-label-2", "Entity-entitySets-2", "Entity-description-2", "Link"},
		{"Person", "e-1", "Smith, Bob [Set-1]", "Set-1", "Bob Smith can be found at http://network-display/e-1", "Location", "e-3", "31 Field Drive, EH36 5PB []", "", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3", "1 docs (Doc-A; 09/08/2022)"},
		{"Location", "e-3", "31 Field Drive, EH36 5PB []", "", "31 Field Drive, EH36 5PB can be found at http://network-display/e-3", "Person", "e-4", "Taylor, Samuel [Set-1]", "Set-1", "Samuel Taylor can be found at http://network-display/e-4", "1 docs (Doc-A; 10/08/2022)"}}
	actualTable, err := i2chart.ReadFromExcel(j1.ResultFile, "Sheet1")
	assert.NoError(t, err)
	assert.Equal(t, expectedTable, actualTable)
}
