package system

import (
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

const componentName = "system"

// A JobRunner is responsible for finding the paths and generating an Excel file for i2.
type JobRunner struct {
	pathFinder   *bfs.PathFinder         // Path finder
	chartBuilder *i2chart.I2ChartBuilder // i2 chart builder
	folder       string                  // Location for the Excel files

	jobs     map[string]*job.Job // Jobs (mapping of guid to job)
	jobsLock sync.RWMutex        // Mutex for the jobs map
}

var (
	ErrPathFinderIsNil    = fmt.Errorf("Pathfinder is nil")
	ErrChartBuilderIsNil  = fmt.Errorf("Chartbuilder is nil")
	ErrJobNotFound        = fmt.Errorf("Job not found")
	ErrJobConfIsNil       = fmt.Errorf("Job configuration is nil")
	ErrFolderDoesNotExist = fmt.Errorf("Folder doesn't exist")
)

// GUID returned on failure
const InvalidGUID = "invalid-guid"

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
		pathFinder:   pathFinder,
		chartBuilder: chartBuilder,
		folder:       folder,
	}, nil
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

	// Execute the job (in a go routine)
	go j.executeJob(job.GUID)

	return job.GUID, nil
}

// setJobToInProgress sets the job to in progress (i.e. started).
func (j *JobRunner) setJobToInProgress(j1 *job.Job) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	j1.Progress.StartTime = time.Now()
	j1.Progress.Status = job.InProgress
}

// setJobToFailed sets the job to failed and stores the error.
func (j *JobRunner) setJobToFailed(failedJob *job.Job, err error) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	failedJob.Progress.Status = job.Failed
	failedJob.Progress.EndTime = time.Now()
	failedJob.Error = err
}

// setJobToComplete sets the job to complete (finished).
func (j *JobRunner) setJobToComplete(j1 *job.Job, filepath string) {
	j.jobsLock.Lock()
	defer j.jobsLock.Unlock()

	j1.Progress.EndTime = time.Now()
	j1.Progress.Status = job.Complete
	j1.ResultFile = filepath
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
			Str("jobGUID", guid).
			Msg("Failed to find job")
	}

	// Set the job to in progress
	j.setJobToInProgress(job)

	// Find the paths between entities
	conns, err := j.pathFinder.FindPaths(job.Configuration.EntitySets, job.Configuration.MaxNumberHops)
	if err != nil {
		j.setJobToFailed(job, err)
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

	j.setJobToComplete(job, filepath)
}

// GetJob from the job runner in a thread-safe manner.
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
