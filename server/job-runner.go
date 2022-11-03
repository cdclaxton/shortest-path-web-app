package server

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

var (
	ErrPathFinderIsNil    = errors.New("pathfinder is nil")
	ErrChartBuilderIsNil  = errors.New("chartbuilder is nil")
	ErrJobNotFound        = errors.New("job not found")
	ErrJobConfIsNil       = errors.New("job configuration is nil")
	ErrFolderDoesNotExist = errors.New("folder doesn't exist")
	ErrInvalidGuid        = errors.New("invalid GUID")
)

// GUID returned on failure (instead of an empty string)
const InvalidGUID = "invalid-guid"

// Message to display to the user when no paths between entities were found
const noPathsMessage = "Sorry, no paths were found between entities. Maybe increase the number of hops."

// A JobRunner is responsible for finding the paths and generating an Excel file for i2.
type JobRunner struct {
	pathFinder   *bfs.PathFinder         // Path finder
	chartBuilder *i2chart.I2ChartBuilder // i2 chart builder
	folder       string                  // Location for the Excel files

	jobs     map[string]*job.Job // Jobs (mapping of guid to job)
	jobsLock sync.RWMutex        // Mutex for the jobs map

	numberJobsExecuting     int          // Number of jobs being executed
	numberJobsExecutingLock sync.RWMutex // Mutex for the numberJobsExecuting
}

// NewJobRunner instantiates a new JobRunner struct.
func NewJobRunner(pathFinder *bfs.PathFinder, chartBuilder *i2chart.I2ChartBuilder,
	folder string) (*JobRunner, error) {

	// Preconditions
	if pathFinder == nil {
		return nil, ErrPathFinderIsNil
	}

	if chartBuilder == nil {
		return nil, ErrChartBuilderIsNil
	}

	if _, err := os.Stat(folder); os.IsNotExist(err) {
		return nil, ErrFolderDoesNotExist
	}

	// Return a constructed job runner
	return &JobRunner{
		pathFinder:              pathFinder,
		chartBuilder:            chartBuilder,
		folder:                  folder,
		jobs:                    map[string]*job.Job{},
		jobsLock:                sync.RWMutex{},
		numberJobsExecuting:     0,
		numberJobsExecutingLock: sync.RWMutex{},
	}, nil
}

// goingToExecuteJob increments the number of jobs executing.
func (j *JobRunner) goingToExecuteJob(guid string) {
	j.numberJobsExecutingLock.Lock()
	defer j.numberJobsExecutingLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Going to execute job")

	j.numberJobsExecuting += 1
}

// finishedExecutingJob decrements the number of jobs executing.
func (j *JobRunner) finishedExecutingJob(guid string) {
	j.numberJobsExecutingLock.Lock()
	defer j.numberJobsExecutingLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Finished executing job")

	j.numberJobsExecuting -= 1
}

// GetNumberJobsExecuting returns the number of jobs being executed when the lock is acquired.
func (j *JobRunner) GetNumberJobsExecuting() int {
	j.numberJobsExecutingLock.RLock()
	defer j.numberJobsExecutingLock.RUnlock()

	return j.numberJobsExecuting
}

// addJob to the map of jobs once the write lock has been acquired.
func (j *JobRunner) addJob(j1 *job.Job) error {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	if !j1.HasValidGuid() {
		return ErrInvalidGuid
	}

	j.jobs[j1.GUID] = j1
	return nil
}

// Submit the job for execution.
func (j *JobRunner) Submit(jobConf *job.JobConfiguration) (string, error) {

	// Preconditions
	if jobConf == nil {
		return InvalidGUID, ErrJobConfIsNil
	}

	// Create the job
	job, err := job.NewJob(jobConf)
	if err != nil {
		return InvalidGUID, err
	}

	// Add the job to the job runner's storage
	err = j.addJob(&job)
	if err != nil {
		return InvalidGUID, err
	}

	// Execute the job (in a go routine)
	j.goingToExecuteJob(job.GUID)
	go j.executeJob(job.GUID)

	return job.GUID, nil
}

// setJobToInProgress sets the job to in progress (i.e. started).
func (j *JobRunner) setJobToInProgress(j1 *job.Job) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting job to in progress")

	j1.Progress.StartTime = time.Now()
	j1.Progress.State = job.InProgress
}

// setJobToFailed sets the job to failed and stores the error in the job.
func (j *JobRunner) setJobToFailed(failedJob *job.Job, err error) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, failedJob.GUID).
		Msg("Setting job to failed")

	failedJob.Progress.State = job.Failed
	failedJob.Progress.EndTime = time.Now()
	failedJob.Error = err

	j.finishedExecutingJob(failedJob.GUID)
}

// setJobToComplete sets the job to complete (finished) where there were results.
func (j *JobRunner) setJobToCompleteResults(j1 *job.Job, filepath string) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting job to complete with results")

	j1.Progress.EndTime = time.Now()
	j1.Progress.State = job.CompleteResults
	j1.ResultFile = filepath

	j.finishedExecutingJob(j1.GUID)
}

// setJobToCompleteNoResults sets the job to complete (finished) where there weren't any results.
func (j *JobRunner) setJobToCompleteNoResults(j1 *job.Job) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting job to complete with no results")

	j1.Progress.EndTime = time.Now()
	j1.Progress.State = job.CompleteNoResults
	j1.Message = noPathsMessage

	j.finishedExecutingJob(j1.GUID)
}

// makeExcelFilepath for storage of the Excel file.
func makeExcelFilepath(folder string, guid string) string {
	return path.Join(folder, fmt.Sprintf("%v.xlsx", guid))
}

// executeJob given the GUID of the job to execute.
func (j *JobRunner) executeJob(guid string) {

	// Get the job
	job, err := j.GetJob(guid)
	if err != nil {
		logging.Logger.Warn().
			Str(logging.ComponentField, componentName).
			Str(loggingGUIDField, guid).
			Msg("Failed to find job")
		return
	}

	// Set the job to in progress
	j.setJobToInProgress(job)

	// Find the paths between entities
	conns, err := j.pathFinder.FindPaths(job.Configuration.EntitySets, job.Configuration.MaxNumberHops)
	if err != nil {
		j.setJobToFailed(job, err)
		return
	}

	// If there aren't any connections, there's no need to build the i2 chart
	if !conns.HasAnyConnections() {
		j.setJobToCompleteNoResults(job)
		return
	}

	// Build the i2 chart (as a table)
	table, err := j.chartBuilder.Build(conns)
	if err != nil {
		j.setJobToFailed(job, err)
		return
	}

	// Make the filepath for the Excel file
	filepath := makeExcelFilepath(j.folder, guid)

	// Save the table in an Excel file
	err = i2chart.WriteToExcel(filepath, table)
	if err != nil {
		j.setJobToFailed(job, err)
		return
	}

	j.setJobToCompleteResults(job, filepath)
}

// GetJob from the job runner in a thread-safe manner. The returned job should not be modified.
func (j *JobRunner) GetJob(guid string) (*job.Job, error) {

	// Get a lock to be able to read the jobs map
	j.jobsLock.RLock()
	defer j.jobsLock.RUnlock()

	// Try to fetch the job
	job, found := j.jobs[guid]
	if !found {
		return nil, ErrJobNotFound
	}

	return job, nil
}

// IsJobFinished given the job's GUID.
func (j *JobRunner) IsJobFinished(guid string) (bool, error) {

	// Get a lock to be able to read the jobs map
	j.jobsLock.RLock()
	defer j.jobsLock.RUnlock()

	// Try to fetch the job
	j1, found := j.jobs[guid]
	if !found {
		return false, ErrJobNotFound
	}

	// If the job is in an end state, it is finished
	if j1.Progress.State == job.Failed ||
		j1.Progress.State == job.CompleteNoResults ||
		j1.Progress.State == job.CompleteResults {

		return true, nil
	} else {
		return false, nil
	}
}
