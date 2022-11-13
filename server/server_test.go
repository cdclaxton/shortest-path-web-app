package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/stretchr/testify/assert"
)

func TestSplitEntityIDs(t *testing.T) {
	testCases := []struct {
		text     string
		expected []string
	}{
		{
			text:     "",
			expected: []string{},
		},
		{
			text:     "123",
			expected: []string{"123"},
		},
		{
			text:     "123 234",
			expected: []string{"123", "234"},
		},
		{
			text:     "123,234",
			expected: []string{"123", "234"},
		},
		{
			text:     "123\n234",
			expected: []string{"123", "234"},
		},
		{
			text:     "123\t234",
			expected: []string{"123", "234"},
		},
		{
			text:     "123;234",
			expected: []string{"123", "234"},
		},
		{
			text:     "123;234,345\n456\t567 678",
			expected: []string{"123", "234", "345", "456", "567", "678"},
		},
	}

	for _, testCase := range testCases {
		actual := splitEntityIDs(testCase.text)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestParseNumberOfHops(t *testing.T) {

	testCases := []struct {
		numberHopsOnForm   string
		numberHopsExpected int
		errorExpected      bool
	}{
		{
			numberHopsOnForm:   "",
			numberHopsExpected: 0,
			errorExpected:      true,
		},
		{
			numberHopsOnForm:   "1",
			numberHopsExpected: 1,
			errorExpected:      false,
		},
		{
			numberHopsOnForm:   fmt.Sprintf("%v", MinimumNumberHops-1), // Invalid number of hops
			numberHopsExpected: 0,
			errorExpected:      true,
		},
		{
			numberHopsOnForm:   fmt.Sprintf("%v", MaximumNumberHops+1), // Invalid number of hops
			numberHopsExpected: 0,
			errorExpected:      true,
		},
		{
			numberHopsOnForm:   "abc", // Invalid number of hops
			numberHopsExpected: 0,
			errorExpected:      true,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(NumberHopsInputName, testCase.numberHopsOnForm)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
		req.Form = form

		result, err := parseNumberOfHops(req)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.numberHopsExpected, result)
	}

}

func TestParseDataset(t *testing.T) {

	testCases := []struct {
		datasetIndex  int
		name1         string
		entityIds1    string
		name2         string
		entityIds2    string
		expected      *job.EntitySet
		errorExpected bool
	}{
		{
			datasetIndex:  -1, // Invalid dataset index
			name1:         "Dataset 1",
			entityIds1:    "1234",
			name2:         "",
			entityIds2:    "",
			expected:      nil,
			errorExpected: true,
		},
		{
			datasetIndex: 1,
			name1:        "Dataset 1",
			entityIds1:   "1234",
			name2:        "",
			entityIds2:   "",
			expected: &job.EntitySet{
				Name:      "Dataset 1",
				EntityIds: []string{"1234"},
			},
			errorExpected: false,
		},
		{
			datasetIndex:  2,
			name1:         "Dataset 1",
			entityIds1:    "1234",
			name2:         "",
			entityIds2:    "",
			expected:      nil,
			errorExpected: false,
		},
		{
			datasetIndex: 2,
			name1:        "Dataset 1",
			entityIds1:   "1234",
			name2:        "Dataset 2",
			entityIds2:   "2345;3456",
			expected: &job.EntitySet{
				Name:      "Dataset 2",
				EntityIds: []string{"2345", "3456"},
			},
			errorExpected: false,
		},
		{
			datasetIndex:  1,
			name1:         "Dataset 1", // Name, but no entity IDs
			entityIds1:    "",
			name2:         "",
			entityIds2:    "",
			expected:      nil,
			errorExpected: true,
		},
		{
			datasetIndex:  1,
			name1:         "",
			entityIds1:    "1234", // Entity IDs but no name
			name2:         "",
			entityIds2:    "",
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 1), testCase.name1)
		form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 1), testCase.entityIds1)
		form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 2), testCase.name2)
		form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 2), testCase.entityIds2)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
		req.Form = form

		// Try to parse an entity set from the form data
		actual, err := parseEntitySet(req, testCase.datasetIndex)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestExtractJobConfigurationFromForm(t *testing.T) {

	testCases := []struct {
		maxHops         string
		name1           string
		entityIds1      string
		name2           string
		entityIds2      string
		maxDatasetIndex int
		expected        *job.JobConfiguration
		errorExpected   bool
	}{
		{
			maxHops:         "0", // Invalid number of hops
			name1:           "Dataset 1",
			entityIds1:      "1234",
			name2:           "Dataset 2",
			entityIds2:      "2345",
			maxDatasetIndex: 2,
			expected:        nil,
			errorExpected:   true,
		},
		{
			maxHops:         "1", // No entity sets
			name1:           "",
			entityIds1:      "",
			name2:           "",
			entityIds2:      "",
			maxDatasetIndex: 2,
			expected:        nil,
			errorExpected:   true,
		},
		{
			maxHops:         "1",
			name1:           "Dataset 1",
			entityIds1:      "1234",
			name2:           "Dataset 2",
			entityIds2:      "2345",
			maxDatasetIndex: 1,
			expected: &job.JobConfiguration{
				MaxNumberHops: 1,
				EntitySets: []job.EntitySet{
					{
						Name:      "Dataset 1",
						EntityIds: []string{"1234"},
					},
				},
			},
			errorExpected: false,
		},
		{
			maxHops:         "1",
			name1:           "Dataset 1",
			entityIds1:      "1234",
			name2:           "Dataset 2",
			entityIds2:      "2345",
			maxDatasetIndex: 2,
			expected: &job.JobConfiguration{
				MaxNumberHops: 1,
				EntitySets: []job.EntitySet{
					{
						Name:      "Dataset 1",
						EntityIds: []string{"1234"},
					},
					{
						Name:      "Dataset 2",
						EntityIds: []string{"2345"},
					},
				},
			},
			errorExpected: false,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(NumberHopsInputName, testCase.maxHops)
		form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 1), testCase.name1)
		form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 1), testCase.entityIds1)
		form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 2), testCase.name2)
		form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 2), testCase.entityIds2)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
		req.Form = form

		// Try to parse an entity set from the form data
		actual, err := extractJobConfigurationFromForm(req, testCase.maxDatasetIndex)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestBuildFilename(t *testing.T) {
	testCases := []struct {
		jobConf          *job.JobConfiguration
		errorExpected    bool
		expectedFilename string
	}{
		{
			jobConf: &job.JobConfiguration{
				EntitySets: []job.EntitySet{
					{
						Name: "dataset A",
					},
				},
				MaxNumberHops: 1,
			},
			errorExpected:    false,
			expectedFilename: "shortest-path - dataset A - 1 hop.xlsx",
		},
		{
			jobConf: &job.JobConfiguration{
				EntitySets: []job.EntitySet{
					{
						Name: "dataset A",
					},
				},
				MaxNumberHops: 2,
			},
			errorExpected:    false,
			expectedFilename: "shortest-path - dataset A - 2 hops.xlsx",
		},
		{
			jobConf: &job.JobConfiguration{
				EntitySets: []job.EntitySet{
					{
						Name: "dataset A",
					},
					{
						Name: "dataset B",
					},
				},
				MaxNumberHops: 1,
			},
			errorExpected:    false,
			expectedFilename: "shortest-path - dataset A - dataset B - 1 hop.xlsx",
		},
		{
			jobConf: &job.JobConfiguration{
				EntitySets:    nil,
				MaxNumberHops: 1,
			},
			errorExpected:    true,
			expectedFilename: "",
		},
		{
			jobConf: &job.JobConfiguration{
				EntitySets:    []job.EntitySet{},
				MaxNumberHops: 1,
			},
			errorExpected:    true,
			expectedFilename: "",
		},
	}

	for _, testCase := range testCases {
		actual, err := buildFilename(testCase.jobConf)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.Nil(t, err)
		}

		assert.Equal(t, testCase.expectedFilename, actual)
	}
}

func makeJobServer(t *testing.T) *JobServer {

	// Make a well-configured job runner
	runner := makeJobRunner(t)

	// Make a Job server that is correctly configured
	server, err := NewJobServer(runner)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	return server
}

func TestHandleInvalidDownload(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Try to download without a GUID
	req := httptest.NewRequest(http.MethodGet, "/download", strings.NewReader(""))
	w := httptest.NewRecorder()

	server.handleDownload(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Try to download for a GUID that doesn't exist
	req = httptest.NewRequest(http.MethodGet, "/download/1234", strings.NewReader(""))
	w = httptest.NewRecorder()

	server.handleDownload(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// buildFormData for the /upload endpoint for testing purposes.
func buildFormData(maxNumberHops int,
	dataset1 string, entities1 string,
	dataset2 string, entities2 string,
	dataset3 string, entities3 string) url.Values {

	form := url.Values{}
	form.Add(NumberHopsInputName, strconv.Itoa(maxNumberHops))
	form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 1), dataset1)
	form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 1), entities1)
	form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 2), dataset2)
	form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 2), entities2)
	form.Add(fmt.Sprintf("%v%v", DatasetNameInputName, 3), dataset3)
	form.Add(fmt.Sprintf("%v%v", DatasetEntitiesInputName, 3), entities3)

	return form
}

func TestUploadInvalidConfiguration(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Upload an empty form
	req := httptest.NewRequest(http.MethodGet, "/upload", strings.NewReader(""))
	w := httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// Upload a form with one dataset, but no entity IDs
	form := buildFormData(1, "Dataset-1", "", "", "", "", "")
	req = httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	req.Form = form
	w = httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// extractGuidFromLocation returns the job GUID from a path for the form /job/<GUID>.
func extractGuidFromLocation(t *testing.T, location string) string {
	assert.True(t, strings.Contains(location, "/job/"))

	pattern := regexp.MustCompile("^/job/(.*)")
	matches := pattern.FindStringSubmatch(location)
	assert.Equal(t, 2, len(matches))

	return matches[1]
}

// isNoResultsPage returns true if the HTTP response shows that there are no results for the job.
func isNoResultsPage(w *httptest.ResponseRecorder, guid string) bool {

	// Extract the body of the HTTP response as a string
	body := w.Body.String()

	// Check that the (correct) GUID is present
	if !strings.Contains(body, guid) {
		return false
	}

	// Check the text
	return strings.Contains(body, "No results")
}

// isPageWithResults returns true if the HTTP response shows that there are results for the job.
func isPageWithResults(w *httptest.ResponseRecorder, guid string) bool {

	// Extract the body of the HTTP response as a string
	body := w.Body.String()

	// Check that the (correct) GUID is present
	if !strings.Contains(body, guid) {
		return false
	}

	// Check the text
	return strings.Contains(body, "Download Excel file")
}

// isErrorPage returns true if the HTTP resoonse shows that the job errored.
func isErrorPage(w *httptest.ResponseRecorder, guid string) bool {

	// Extract the body of the HTTP response as a string
	body := w.Body.String()

	// Check that the (correct) GUID is present
	if !strings.Contains(body, guid) {
		return false
	}

	// Check the text
	return strings.Contains(body, "Job failed")
}

// isJobNotFoundPage returns true if the HTTP resoonse shows that the job could not be found.
func isJobNotFoundPage(w *httptest.ResponseRecorder, guid string) bool {

	// Extract the body of the HTTP response as a string
	body := w.Body.String()

	// Check that the (correct) GUID is present
	if !strings.Contains(body, guid) {
		return false
	}

	// Check the text
	return strings.Contains(body, "Oops! Job not found")
}

func TestUploadNoResults(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Upload a form with one dataset, but no matching entity IDs
	form := buildFormData(1, "Dataset-1", "e-100,e-102", "", "", "", "")
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	req.Form = form

	w := httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusFound, w.Code)

	// Extract the location
	location := w.Result().Header.Get("Location")
	assert.True(t, len(location) > 0)
	assert.True(t, strings.HasPrefix(location, "/job/"))

	// Get the job GUID from the location
	guid := extractGuidFromLocation(t, location)

	// Wait until the job is complete
	waitForJobsToFinish(server.runner)

	// Request the job
	req = httptest.NewRequest(http.MethodGet, location, nil)
	w = httptest.NewRecorder()

	server.handleJob(w, req)
	assert.True(t, isNoResultsPage(w, guid))
}

func TestUploadWithResults(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Upload a form with one dataset, but no matching entity IDs
	form := buildFormData(1, "Dataset-1", "e-1, e-2", "", "", "", "")
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	req.Form = form

	w := httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusFound, w.Code)

	// Extract the location
	location := w.Result().Header.Get("Location")
	assert.True(t, len(location) > 0)
	assert.True(t, strings.HasPrefix(location, "/job/"))

	// Get the job GUID from the location
	guid := extractGuidFromLocation(t, location)

	// Wait until the job is complete
	waitForJobsToFinish(server.runner)

	// Request the job
	req = httptest.NewRequest(http.MethodGet, location, nil)
	w = httptest.NewRecorder()

	server.handleJob(w, req)
	assert.True(t, len(w.Body.String()) > 0)
	assert.True(t, isPageWithResults(w, guid))
}

func TestDownloadWithResults(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Upload a form with one dataset, but no matching entity IDs
	form := buildFormData(1, "Dataset-1", "e-1, e-2", "", "", "", "")
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	req.Form = form

	w := httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusFound, w.Code)

	// Extract the location
	location := w.Result().Header.Get("Location")
	assert.True(t, len(location) > 0)
	assert.True(t, strings.HasPrefix(location, "/job/"))

	// Get the job GUID from the location
	guid := extractGuidFromLocation(t, location)

	// Wait until the job is complete
	waitForJobsToFinish(server.runner)

	// Try to download the results
	url := fmt.Sprintf("/download/%v", guid)
	req = httptest.NewRequest(http.MethodGet, url, nil)
	w = httptest.NewRecorder()

	server.handleDownload(w, req)
	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.True(t, len(w.Body.String()) > 0)

	disposition := w.Result().Header.Get("Content-Disposition")
	assert.Equal(t, "attachment; filename=shortest-path - Dataset-1 - 1 hop.xlsx", disposition)
}

func TestUploadFailedJob(t *testing.T) {

	// Make a valid job server, but remove the folder from the job runner so that the job errors
	server := makeJobServer(t)
	cleanUpJobRunner(t, server.runner)

	// Upload a form with one dataset, but no matching entity IDs
	form := buildFormData(1, "Dataset-1", "e-1, e-2", "", "", "", "")
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	req.Form = form

	w := httptest.NewRecorder()

	server.handleUpload(w, req)
	assert.Equal(t, http.StatusFound, w.Code)

	// Extract the location
	location := w.Result().Header.Get("Location")
	assert.True(t, len(location) > 0)
	assert.True(t, strings.HasPrefix(location, "/job/"))

	// Get the job GUID from the location
	guid := extractGuidFromLocation(t, location)

	// Wait until the job is complete
	waitForJobsToFinish(server.runner)

	// Request the job
	req = httptest.NewRequest(http.MethodGet, location, nil)
	w = httptest.NewRecorder()

	server.handleJob(w, req)
	assert.True(t, len(w.Body.String()) > 0)
	assert.True(t, isErrorPage(w, guid))
}

func TestHandleJobInvalidJob(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Request the job
	req := httptest.NewRequest(http.MethodGet, "/job/1234", nil)
	w := httptest.NewRecorder()

	server.handleJob(w, req)
	assert.True(t, len(w.Body.String()) > 0)
	assert.True(t, isJobNotFoundPage(w, "1234"))
}
