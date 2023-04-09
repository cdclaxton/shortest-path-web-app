package server

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/spider"
)

var (
	ErrSpiderIsNil             = errors.New("spider engine is nil")
	ErrSpiderChartBuilderIsNil = errors.New("spider chart builder is nil")
)

// A SpiderJobRunner is responsible for spidering and generating an Excel file for i2.
type SpiderJobRunner struct {
	spider       *spider.Spider              // Spider engine
	chartBuilder *i2chart.SpiderChartBuilder // Spider chart builder
	folder       string                      // Location for the Excel files

	jobs     map[string]*job.SpiderJob // Jobs (mapping of guid to job)
	jobsLock sync.RWMutex              // Mutex for the jobs map

	numberJobsExecuting     int          // Number of jobs being executed
	numberJobsExecutingLock sync.RWMutex // Mutex for the numberJobsExecuting
}

// NewJobRunner instantiates a new SpiderJobRunner struct.
func NewSpiderJobRunner(spider *spider.Spider, chartBuilder *i2chart.SpiderChartBuilder,
	folder string) (*SpiderJobRunner, error) {

	if spider == nil {
		return nil, ErrSpiderIsNil
	}

	if chartBuilder == nil {
		return nil, ErrSpiderChartBuilderIsNil
	}

	if _, err := os.Stat(folder); os.IsNotExist(err) {
		return nil, ErrFolderDoesNotExist
	}

	// Return a constructed job runner
	return &SpiderJobRunner{
		spider:                  spider,
		chartBuilder:            chartBuilder,
		folder:                  folder,
		jobs:                    map[string]*job.SpiderJob{},
		jobsLock:                sync.RWMutex{},
		numberJobsExecuting:     0,
		numberJobsExecutingLock: sync.RWMutex{},
	}, nil
}

// goingToExecuteJob increments the number of jobs executing.
func (j *SpiderJobRunner) goingToExecuteJob(guid string) {
	j.numberJobsExecutingLock.Lock()
	defer j.numberJobsExecutingLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Going to execute spider job")

	j.numberJobsExecuting += 1
}

// finishedExecutingJob decrements the number of jobs executing.
func (j *SpiderJobRunner) finishedExecutingJob(guid string) {
	j.numberJobsExecutingLock.Lock()
	defer j.numberJobsExecutingLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Finished executing spider job")

	j.numberJobsExecuting -= 1
}

// GetNumberJobsExecuting returns the number of jobs being executed when the lock is acquired.
func (j *SpiderJobRunner) GetNumberJobsExecuting() int {
	j.numberJobsExecutingLock.RLock()
	defer j.numberJobsExecutingLock.RUnlock()

	return j.numberJobsExecuting
}

// addJob to the map of jobs once the write lock has been acquired.
func (j *SpiderJobRunner) addJob(j1 *job.SpiderJob) error {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	if !j1.HasValidGuid() {
		return ErrInvalidGuid
	}

	j.jobs[j1.GUID] = j1
	return nil
}

// Submit the job for execution.
func (j *SpiderJobRunner) Submit(jobConf *job.SpiderJobConfiguration) (string, error) {

	// Preconditions
	if jobConf == nil {
		return InvalidGUID, ErrJobConfIsNil
	}

	// Create the job
	job, err := job.NewSpiderJob(jobConf)
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
func (j *SpiderJobRunner) setJobToInProgress(j1 *job.SpiderJob) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting spider job to in progress")

	j1.Progress.StartTime = time.Now()
	j1.Progress.State = job.InProgress
}

// setJobToFailed sets the job to failed and stores the error in the job.
func (j *SpiderJobRunner) setJobToFailed(failedJob *job.SpiderJob, err error) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, failedJob.GUID).
		Str("error", err.Error()).
		Msg("Setting spider job to failed")

	failedJob.Progress.State = job.Failed
	failedJob.Progress.EndTime = time.Now()
	failedJob.Error = err

	j.finishedExecutingJob(failedJob.GUID)
}

// setJobToComplete sets the job to complete (finished) where there were results.
func (j *SpiderJobRunner) setJobToCompleteResults(j1 *job.SpiderJob, filepath string) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting spider job to complete with results")

	j1.Progress.EndTime = time.Now()
	j1.Progress.State = job.CompleteResults
	j1.ResultFile = filepath

	j.finishedExecutingJob(j1.GUID)
}

// setJobToCompleteNoResults sets the job to complete (finished) where there weren't any results.
func (j *SpiderJobRunner) setJobToCompleteNoResults(j1 *job.SpiderJob) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, j1.GUID).
		Msg("Setting spider job to complete with no results")

	j1.Progress.EndTime = time.Now()
	j1.Progress.State = job.CompleteNoResults
	j1.Message = noPathsMessage

	j.finishedExecutingJob(j1.GUID)
}

// executeJob given the GUID of the job to execute.
func (j *SpiderJobRunner) executeJob(guid string) {

	// Get the job
	job, err := j.GetJob(guid)
	if err != nil {
		logging.Logger.Warn().
			Str(logging.ComponentField, componentName).
			Str(loggingGUIDField, guid).
			Msg("Failed to find spider job")
		return
	}

	// Set the job to in progress
	j.setJobToInProgress(job)

	// Perform spidering
	results, err := j.spider.Execute(job.Configuration.NumberSteps, job.Configuration.SeedEntities)
	if err != nil {
		j.setJobToFailed(job, err)
		return
	}

	// If there aren't any connections, there's no need to build the i2 chart
	atLeastOneConnection, err := results.HasAtLeastOneConnection()
	if err != nil {
		j.setJobToFailed(job, err)
		return
	}
	if !atLeastOneConnection {
		j.setJobToCompleteNoResults(job)
		return
	}

	// Build the i2 chart (as a table)
	table, err := j.chartBuilder.Build(results)
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
func (j *SpiderJobRunner) GetJob(guid string) (*job.SpiderJob, error) {

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
func (j *SpiderJobRunner) IsJobFinished(guid string) (bool, error) {

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
