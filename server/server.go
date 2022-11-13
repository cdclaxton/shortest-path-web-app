package server

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aymerick/raymond"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

// Component name used in logging
const componentName = "server"

// Field used in logging for the job
const loggingGUIDField = "jobGUID"

// Constants associated with the upload (form) page
const (
	MinimumNumberHops        = 1                 // Minimum number of hops from an entity to another
	MaximumNumberHops        = 5                 // Maximum number of hops from an entity to another
	MaxDatasetIndex          = 3                 // Maximum number of datasets on the frontend
	NumberHopsInputName      = "numberHops"      // Name of select box for number of hops
	DatasetNameInputName     = "datasetName"     // Prefix of the name of the text box for the dataset name
	DatasetEntitiesInputName = "datasetEntities" // Prefix of the name of the text box containing entity IDs
)

// Locations of the HTML templates
const (
	errorTemplateFile         = "templates/error.html"          // For a system error
	inputProblemTemplateFile  = "templates/input-problem.html"  // For a data error
	jobNotFoundTemplateFile   = "templates/job-not-found.html"  // For when a job cannot be found
	processingJobTemplateFile = "templates/processing-job.html" // For during processing
	jobFailedTemplateFile     = "templates/job-failed.html"     // For a failed job
	jobNoResultsTemplateFile  = "templates/job-no-results.html" // For a complete job
	jobResultsTemplateFile    = "templates/job-results.html"    // For a complete job
)

// A JobServer is responsible for providing the HTTP endpoints for running jobs.
type JobServer struct {
	runner                *JobRunner        // Job runner
	errorTemplate         *raymond.Template // Template if a system error occurs
	inputProblemTemplate  *raymond.Template // Template if there is a problem with the user input
	jobNotFoundTemplate   *raymond.Template // Template if the job couldn't be found
	processingJobTemplate *raymond.Template // Template whilst the job is processing
	jobFailedTemplate     *raymond.Template // Template for a failed job
	jobNoResultsTemplate  *raymond.Template // Template if the job completed and there are no results
	jobResultsTemplate    *raymond.Template // Template if the job completed and there are results
}

//go:embed templates/*
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

// readTemplate from an embedded file.
func readTemplate(filepath string) (*raymond.Template, error) {

	// Read the file from the embedded files
	bytes, err := templatesFS.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	// Convert the bytes to a string
	templateString := string(bytes)

	// Parse the template
	return raymond.Parse(templateString)
}

// NewJobServer given the job runner for executing jobs. It will return an error if any of the
// required HTML templates cannot be located.
func NewJobServer(runner *JobRunner) (*JobServer, error) {

	// Preconditions
	if runner == nil {
		return nil, errors.New("job runner is nil")
	}

	// Read the templates
	errorTemplate, err := readTemplate(errorTemplateFile)
	if err != nil {
		return nil, err
	}

	inputProblemTemplate, err := readTemplate(inputProblemTemplateFile)
	if err != nil {
		return nil, err
	}

	jobNotFoundTemplate, err := readTemplate(jobNotFoundTemplateFile)
	if err != nil {
		return nil, err
	}

	processingJobTemplate, err := readTemplate(processingJobTemplateFile)
	if err != nil {
		return nil, err
	}

	jobFailedTemplate, err := readTemplate(jobFailedTemplateFile)
	if err != nil {
		return nil, err
	}

	jobNoResultsTemplate, err := readTemplate(jobNoResultsTemplateFile)
	if err != nil {
		return nil, err
	}

	jobResultsTemplate, err := readTemplate(jobResultsTemplateFile)
	if err != nil {
		return nil, err
	}

	// Return the constructed job server
	return &JobServer{
		runner:                runner,
		errorTemplate:         errorTemplate,
		inputProblemTemplate:  inputProblemTemplate,
		jobNotFoundTemplate:   jobNotFoundTemplate,
		processingJobTemplate: processingJobTemplate,
		jobFailedTemplate:     jobFailedTemplate,
		jobNoResultsTemplate:  jobNoResultsTemplate,
		jobResultsTemplate:    jobResultsTemplate,
	}, nil
}

// parseNumberOfHops in the HTTP POST form data.
func parseNumberOfHops(req *http.Request) (int, error) {

	// Read the number of hops from the form
	numberHops := req.FormValue(NumberHopsInputName)

	if len(numberHops) == 0 {
		return 0, errors.New("number of hops is blank")
	}

	// Convert the string version of the number of hops to an integer
	value, err := strconv.Atoi(numberHops)
	if err != nil {
		return 0, fmt.Errorf("invalid number of hops: %v", value)
	}

	// Validate the number of hops
	if value < MinimumNumberHops || value > MaximumNumberHops {
		return 0, fmt.Errorf("invalid number of hops: %v", value)
	}

	return value, nil
}

// splitEntityIDs from a string using space, newline, comma and semicolon separators.
func splitEntityIDs(text string) []string {

	// Split the potential entity IDs from the string
	re := regexp.MustCompile("[ ,;\t\n]+")
	potentialEntityIds := re.Split(text, -1)

	// Retain entity IDs that pass basic validation
	entityIds := []string{}
	for idx := range potentialEntityIds {
		if len(potentialEntityIds[idx]) > 0 {
			entityIds = append(entityIds, potentialEntityIds[idx])
		}
	}

	return entityIds
}

// Errors that can occur with user-defined datasets
var (
	ErrDatasetNoName     = errors.New("dataset has no name")
	ErrDatasetNoEntities = errors.New("dataset has no entity IDs")
)

// parseEntitySet from the HTTP POST form data.
func parseEntitySet(req *http.Request, index int) (*job.EntitySet, error) {

	// Preconditions
	if req == nil {
		return nil, fmt.Errorf("HTTP request is nil")
	}

	if index < 0 || index > MaxDatasetIndex {
		return nil, fmt.Errorf("invalid dataset index: %v", index)
	}

	// Extract the (user-friendly) name of the dataset from the form
	name := req.FormValue(DatasetNameInputName + strconv.Itoa(index))

	// Extract the entity IDs from the form
	allEntityIds := req.FormValue(DatasetEntitiesInputName + strconv.Itoa(index))
	entityIds := splitEntityIDs(allEntityIds)

	// Determine if the dataset passes minimum validity tests
	hasName := len(name) > 0
	hasEntityIds := len(entityIds) > 0

	if hasName && hasEntityIds {
		return &job.EntitySet{
			Name:      name,
			EntityIds: entityIds,
		}, nil
	} else if hasName && !hasEntityIds {
		return nil, ErrDatasetNoEntities
	} else if !hasName && hasEntityIds {
		return nil, ErrDatasetNoName
	} else {
		return nil, nil
	}
}

// extractJobConfigurationFromForm extracts, parses and validates the configuration for a job.
// If the job would not be valid, return an error message that should be meaningful to the user.
func extractJobConfigurationFromForm(req *http.Request, maxDatasetIndex int) (*job.JobConfiguration, error) {

	// Preconditions
	if req == nil {
		return nil, fmt.Errorf("HTTP request is nil")
	}

	if err := req.ParseForm(); err != nil {
		return nil, fmt.Errorf("unable to parse form: %v", err)
	}

	// Parse the number of hops
	numberHops, err := parseNumberOfHops(req)
	if err != nil {
		return nil, fmt.Errorf("invalid number of hops: %v", err)
	}

	// Initialise the job configuration
	jobConf := job.JobConfiguration{
		MaxNumberHops: numberHops,
		EntitySets:    []job.EntitySet{},
	}

	// Parse the datasets
	for idx := 1; idx <= maxDatasetIndex; idx++ {
		entitySet, err := parseEntitySet(req, idx)

		if err != nil {
			return nil, fmt.Errorf("dataset parse error: %v", err)
		}

		if entitySet != nil {
			jobConf.EntitySets = append(jobConf.EntitySets, *entitySet)
		}
	}

	if len(jobConf.EntitySets) == 0 {
		return nil, fmt.Errorf("there are no datasets")
	}

	return &jobConf, nil
}

func (j *JobServer) handleUpload(w http.ResponseWriter, req *http.Request) {

	// Extract the data from the form
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Handling form upload")
	jobConf, err := extractJobConfigurationFromForm(req, MaxDatasetIndex)

	// If there was an input configuration error, then show the error on a dedicated page
	// and return a 400 error
	if err != nil {

		w.WriteHeader(http.StatusBadRequest)

		page := j.inputProblemTemplate.MustExec(map[string]string{
			"reason": err.Error(),
		})
		fmt.Fprint(w, page)
		return
	}

	// Launch the job. If it fails return a 500 error code
	guid, err := j.runner.Submit(jobConf)
	if err != nil {

		w.WriteHeader(http.StatusInternalServerError)

		page := j.errorTemplate.MustExec(map[string]string{
			"reason": err.Error(),
		})
		fmt.Fprint(w, page)
		return
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Job successfully submitted")

	redirectUrl := fmt.Sprintf("../job/%v", guid)
	http.Redirect(w, req, redirectUrl, http.StatusFound)
}

func (j *JobServer) handleJob(w http.ResponseWriter, req *http.Request) {

	// Extract the guid
	guid := strings.TrimPrefix(req.URL.Path, "/job/")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Received request at /job")

	finished, err := j.runner.IsJobFinished(guid)
	if err == ErrJobNotFound {

		page := j.jobNotFoundTemplate.MustExec(map[string]string{
			"guid": guid,
		})
		fmt.Fprint(w, page)
		return
	}

	if err != nil {
		page := j.errorTemplate.MustExec(map[string]string{
			"reason": err.Error(),
		})
		fmt.Fprint(w, page)
		return
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Str("finished", strconv.FormatBool(finished)).
		Msg("Job completion state")

	if !finished {
		page := j.processingJobTemplate.MustExec(map[string]string{
			"guid": guid,
		})
		fmt.Fprint(w, page)
		return
	}

	// If execution reaches this point, then the job is finished
	// Get the job
	j1, err := j.runner.GetJob(guid)
	if err != nil {
		page := j.errorTemplate.MustExec(map[string]string{
			"reason": err.Error(),
		})
		fmt.Fprint(w, page)
		return
	}

	if j1.Progress.State == job.Failed {

		page := j.jobFailedTemplate.MustExec(map[string]string{
			"reason": j1.Error.Error(),
		})
		fmt.Fprint(w, page)
		return

	} else if j1.Progress.State == job.CompleteNoResults {

		page := j.jobNoResultsTemplate.MustExec(map[string]string{
			"guid": guid,
		})
		fmt.Fprint(w, page)
		return

	} else if j1.Progress.State == job.CompleteResults {

		page := j.jobResultsTemplate.MustExec(map[string]string{
			"guid": guid,
		})
		fmt.Fprint(w, page)
		return
	}

	fmt.Fprintf(w, "Something has gone terribly wrong if you can read this")
}

const resultsFilenamePrefix = "shortest-path - "

// buildFilename for the XLSX results file for download.
func buildFilename(jobConf *job.JobConfiguration) (string, error) {

	// Preconditions
	if jobConf == nil {
		return "", fmt.Errorf("job configuration is nil")
	}

	if len(jobConf.EntitySets) == 0 {
		return "", fmt.Errorf("no entity sets")
	}

	datasetNames := []string{}
	for _, entitySet := range jobConf.EntitySets {
		datasetNames = append(datasetNames, entitySet.Name)
	}

	// Sort the dataset names
	sort.Strings(datasetNames)

	// Build the string part for the number of hops
	var hopsPart string
	if jobConf.MaxNumberHops == 1 {
		hopsPart = fmt.Sprintf(" - %v hop.xlsx", jobConf.MaxNumberHops)
	} else {
		hopsPart = fmt.Sprintf(" - %v hops.xlsx", jobConf.MaxNumberHops)
	}

	// Build the complete filename
	filename := resultsFilenamePrefix +
		strings.Join(datasetNames, " - ") +
		hopsPart

	return filename, nil
}

func (j *JobServer) handleDownload(w http.ResponseWriter, req *http.Request) {

	// Extract the guid
	guid := strings.TrimPrefix(req.URL.Path, "/download/")

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str(loggingGUIDField, guid).
		Msg("Received request at /download")

	j1, err := j.runner.GetJob(guid)
	if err != nil {

		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str(loggingGUIDField, guid).
			Msg("Job not found")

		w.WriteHeader(http.StatusNotFound)
		return
	}

	file, err := os.Open(j1.ResultFile)
	defer file.Close()

	if err != nil {

		logging.Logger.Error().
			Str(logging.ComponentField, componentName).
			Str(loggingGUIDField, guid).
			Msg("Failed to read Excel file for job")

		page := j.jobFailedTemplate.MustExec(map[string]string{
			"reason": fmt.Sprintf("Failed to read Excel file for job %v", guid),
		})

		fmt.Fprint(w, page)
		return
	}

	// Make the filename
	filename, err := buildFilename(j1.Configuration)
	if err != nil {

		logging.Logger.Error().
			Str(logging.ComponentField, componentName).
			Str(loggingGUIDField, guid).
			Err(err).
			Msg("Failed to build filename")

		filename = "shortest-path-results.xlsx"
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%v", filename))
	w.Header().Set("Content-Type", req.Header.Get("Content-Type"))
	io.Copy(w, file)
}

func (j *JobServer) Start() {

	// Uploading job configuration
	http.HandleFunc("/upload", j.handleUpload)

	// Job status
	http.HandleFunc("/job/", j.handleJob)

	// Download results
	http.HandleFunc("/download/", j.handleDownload)

	// Static content
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		logging.Logger.Fatal().Msg("failed to get sub-directory of static")
	}
	http.Handle("/", http.FileServer(http.FS(sub)))

	// Run the server
	http.ListenAndServe(":8090", nil)
}
