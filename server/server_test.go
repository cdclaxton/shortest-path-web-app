package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/graphbuilder"
	"github.com/cdclaxton/shortest-path-web-app/i2chart"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/search"
	"github.com/cdclaxton/shortest-path-web-app/set"
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

	// Make well-configured job runners
	runner, spiderJobRunner := makeJobRunner(t)

	// Make a Job server that is correctly configured
	server, err := NewJobServer(runner, spiderJobRunner, "", graphbuilder.GraphStats{})
	assert.NoError(t, err)
	assert.NotNil(t, server)

	return server
}

func TestHandleInvalidDownload(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	testCases := []struct {
		description string
		url         string
	}{
		{
			description: "download without a GUID",
			url:         "/download",
		},
		{
			description: "download for a GUID that doesn't exist",
			url:         "/download/1234",
		},
		{
			description: "download a spider job result without a GUID",
			url:         "/spider-download",
		},
		{
			description: "download for a spider job GUID that doesn't exist",
			url:         "/spider-download/1234",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, testCase.url, strings.NewReader(""))
			w := httptest.NewRecorder()

			server.handleDownload(w, req)
			assert.Equal(t, http.StatusNotFound, w.Code)
		})
	}
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

func buildSpiderFormData(numberSteps int, seedEntityIDs string) url.Values {
	form := url.Values{}
	form.Add(NumberStepsInputName, strconv.Itoa(numberSteps))
	form.Add(SeedEntitiesInputName, seedEntityIDs)

	return form
}

func TestUploadInvalidConfiguration(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	testCases := []struct {
		description string
		endpoint    string
		form        url.Values
		handler     func(http.ResponseWriter, *http.Request)
	}{
		{
			description: "empty form",
			endpoint:    "/upload",
			form:        url.Values{},
			handler:     server.handleUpload,
		},
		{
			description: "form with one dataset, but no entity IDs",
			endpoint:    "/upload",
			form:        buildFormData(1, "Dataset-1", "", "", "", "", ""),
			handler:     server.handleUpload,
		},
		{
			description: "form with entity IDs, but no dataset name",
			endpoint:    "/upload",
			form:        buildFormData(1, "", "e-1", "", "", "", ""),
			handler:     server.handleUpload,
		},
		{
			description: "spider empty form",
			endpoint:    "/spider-upload",
			form:        url.Values{},
			handler:     server.spiderUpload,
		},
		{
			description: "spider form with a negative number of steps",
			endpoint:    "/spider-upload",
			form:        buildSpiderFormData(-1, ""),
			handler:     server.spiderUpload,
		},
		{
			description: "spider form with no seed entities",
			endpoint:    "/spider-upload",
			form:        buildSpiderFormData(1, ""),
			handler:     server.spiderUpload,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			// Make the HTTP request
			req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(testCase.form.Encode()))
			req.Form = testCase.form

			w := httptest.NewRecorder()
			testCase.handler(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

// extractGuidFromLocation returns the job GUID from a path for the form /job/<GUID>.
func extractGuidFromLocation(t *testing.T, location string) string {
	assert.True(t, strings.Contains(location, "/job/"))

	pattern := regexp.MustCompile("^/job/(.*)")
	matches := pattern.FindStringSubmatch(location)
	assert.Equal(t, 2, len(matches))

	return matches[1]
}

// webPageContainsText returns true if the web-app for the required job and
// the page contains bodyText.
func webPageContainsText(w *httptest.ResponseRecorder, guid string,
	bodyText string) bool {

	// Extract the body of the HTTP response as a string
	body := w.Body.String()

	// Check that the (correct) GUID is present
	if !strings.Contains(body, guid) {
		return false
	}

	return strings.Contains(body, bodyText)
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
	assert.True(t, webPageContainsText(w, guid, "No results"))
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
	assert.True(t, webPageContainsText(w, guid, "Download Excel file"))
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
	assert.True(t, webPageContainsText(w, guid, "Job failed"))
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
	assert.True(t, webPageContainsText(w, "1234", "Oops! Job not found"))
}

func TestHandleStats(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	// Request the job
	req := httptest.NewRequest(http.MethodGet, "/stats/", nil)
	w := httptest.NewRecorder()

	server.handleStats(w, req)
	assert.True(t, len(w.Body.String()) > 0)
	assert.True(t, strings.Contains(w.Body.String(), "Statistics"))
}

func TestPrepareEntitySearchResults(t *testing.T) {

	testCases := []struct {
		results  map[string]search.EntitySearchResult
		expected []EntitySearchResultsDisplay
	}{
		{
			results: map[string]search.EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  false,
				},
			},
			expected: []EntitySearchResultsDisplay{
				{
					EntityId:     "e-1",
					InUnipartite: true,
					InBipartite:  false,
				},
			},
		},
		{
			results: map[string]search.EntitySearchResult{
				"e-1": {
					InUnipartite: true,
					InBipartite:  false,
				},
				"e-2": {
					InUnipartite: false,
					InBipartite:  false,
				},
			},
			expected: []EntitySearchResultsDisplay{
				{
					EntityId:     "e-1",
					InUnipartite: true,
					InBipartite:  false,
				},
				{
					EntityId:     "e-2",
					InUnipartite: false,
					InBipartite:  false,
				},
			},
		},
	}

	for _, testCase := range testCases {
		actual := prepareEntitySearchResults(testCase.results)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestParseNumberOfSteps(t *testing.T) {
	testCases := []struct {
		numberSteps         string
		expectedNumberSteps int
		errorExpected       bool
	}{
		{
			numberSteps:         "-1",
			expectedNumberSteps: 0,
			errorExpected:       true,
		},
		{
			numberSteps:         "0",
			expectedNumberSteps: 0,
			errorExpected:       false,
		},
		{
			numberSteps:         "1",
			expectedNumberSteps: 1,
			errorExpected:       false,
		},
		{
			numberSteps:         strconv.Itoa(MaximumNumberSteps + 1),
			expectedNumberSteps: 0,
			errorExpected:       true,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(NumberStepsInputName, testCase.numberSteps)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/spider-upload", strings.NewReader(form.Encode()))
		req.Form = form

		actual, err := parseNumberOfSteps(req)

		assert.Equal(t, testCase.expectedNumberSteps, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestParseSeedEntities(t *testing.T) {
	testCases := []struct {
		seedEntities  string
		expected      *set.Set[string]
		errorExpected bool
	}{
		{
			seedEntities:  "",
			expected:      nil,
			errorExpected: true,
		},
		{
			seedEntities:  "e-1",
			expected:      set.NewPopulatedSet("e-1"),
			errorExpected: false,
		},
		{
			seedEntities:  "e-1 ",
			expected:      set.NewPopulatedSet("e-1"),
			errorExpected: false,
		},
		{
			seedEntities:  "e-1 e-2",
			expected:      set.NewPopulatedSet("e-1", "e-2"),
			errorExpected: false,
		},
		{
			seedEntities:  "e-1;e-2",
			expected:      set.NewPopulatedSet("e-1", "e-2"),
			errorExpected: false,
		},
		{
			seedEntities:  "e-1\ne-2",
			expected:      set.NewPopulatedSet("e-1", "e-2"),
			errorExpected: false,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(SeedEntitiesInputName, testCase.seedEntities)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/spider-upload", strings.NewReader(form.Encode()))
		req.Form = form

		actual, err := parseSeedEntities(req)
		if testCase.errorExpected {
			assert.Error(t, err)
			assert.Nil(t, actual)
		} else {
			assert.NoError(t, err)
			assert.True(t, testCase.expected.Equal(actual))
		}
	}
}

func TestExtractSpiderJobConfigurationFromForm(t *testing.T) {
	testCases := []struct {
		numberSteps   string
		seedEntities  string
		expected      *job.SpiderJobConfiguration
		errorExpected bool
	}{
		{
			// Invalid number of steps
			numberSteps:   "-1",
			seedEntities:  "e-1",
			expected:      nil,
			errorExpected: true,
		},
		{
			// Invalid seed entities
			numberSteps:   "1",
			seedEntities:  " ",
			expected:      nil,
			errorExpected: true,
		},
		{
			// One seed entity
			numberSteps:  "1",
			seedEntities: "e-1",
			expected: &job.SpiderJobConfiguration{
				NumberSteps:  1,
				SeedEntities: set.NewPopulatedSet("e-1"),
			},
			errorExpected: false,
		},
		{
			// Two seed entities
			numberSteps:  "1",
			seedEntities: "e-1 e-2",
			expected: &job.SpiderJobConfiguration{
				NumberSteps:  1,
				SeedEntities: set.NewPopulatedSet("e-1", "e-2"),
			},
			errorExpected: false,
		},
		{
			// Two seed entities
			numberSteps:  "2",
			seedEntities: "e-1,e-2",
			expected: &job.SpiderJobConfiguration{
				NumberSteps:  2,
				SeedEntities: set.NewPopulatedSet("e-1", "e-2"),
			},
			errorExpected: false,
		},
	}

	for _, testCase := range testCases {

		// Create the form
		form := url.Values{}
		form.Add(SeedEntitiesInputName, testCase.seedEntities)
		form.Add(NumberStepsInputName, testCase.numberSteps)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/spider-upload", strings.NewReader(form.Encode()))
		req.Form = form

		actual, err := extractSpiderJobConfigurationFromForm(req)

		if testCase.errorExpected {
			assert.Error(t, err)
			assert.Nil(t, actual)
		} else {
			assert.NoError(t, err)
			assert.True(t, testCase.expected.Equal(actual))
		}
	}
}

func TestSpiderUpload(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	testCases := []struct {
		numberSteps        string
		seedEntities       string
		expectedStatusCode int
	}{
		{
			// Invalid number of steps
			numberSteps:        "-1",
			seedEntities:       "e-1",
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			// Invalid seed entities
			numberSteps:        "1",
			seedEntities:       " ",
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			// Correct arguments
			numberSteps:        "1",
			seedEntities:       "e-1",
			expectedStatusCode: http.StatusFound,
		},
	}

	for _, testCase := range testCases {
		// Create the form
		form := url.Values{}
		form.Add(SeedEntitiesInputName, testCase.seedEntities)
		form.Add(NumberStepsInputName, testCase.numberSteps)

		// Make the HTTP request
		req := httptest.NewRequest(http.MethodPost, "/spider-upload", strings.NewReader(form.Encode()))
		req.Form = form

		w := httptest.NewRecorder()
		server.spiderUpload(w, req)

		assert.Equal(t, testCase.expectedStatusCode, w.Result().StatusCode)
	}
}

// extractGuidFromLocation returns the job GUID from a path for the form /job/<GUID>.
func extractSpiderGuidFromLocation(t *testing.T, location string) string {
	assert.True(t, strings.Contains(location, "/spider-job/"))

	pattern := regexp.MustCompile("^/spider-job/(.*)")
	matches := pattern.FindStringSubmatch(location)
	assert.Equal(t, 2, len(matches))

	return matches[1]
}

func TestRunSpiderJob(t *testing.T) {

	// Make a valid job server
	server := makeJobServer(t)
	defer cleanUpJobRunner(t, server.runner)

	testCases := []struct {
		description      string     // Test scenario description
		numberSteps      int        // Number of steps to spider
		seedEntities     string     // Seed entity IDs
		jobShouldBeValid bool       // Should the input data result in a valid job?
		resultsExpected  bool       // Should there be a results file to download?
		expectedTable    [][]string // Expected table if there are results
	}{
		{
			description:      "invalid number of steps",
			numberSteps:      -1,
			seedEntities:     "e-1",
			jobShouldBeValid: false,
			resultsExpected:  false,
			expectedTable:    nil,
		},
		{
			description:      "invalid seed entities",
			numberSteps:      1,
			seedEntities:     "",
			jobShouldBeValid: false,
			resultsExpected:  false,
			expectedTable:    nil,
		},
		{
			description:      "one seed entity, no connections",
			numberSteps:      0,
			seedEntities:     "e-1",
			jobShouldBeValid: true,
			resultsExpected:  false,
			expectedTable:    nil,
		},
		{
			description:      "one seed entity, two connections",
			numberSteps:      1,
			seedEntities:     "e-1",
			jobShouldBeValid: true,
			resultsExpected:  true,
			expectedTable: [][]string{
				{"ID-1", "Type-1", "Icon-1", "Label-1", "Seed-1", "ID-2", "Type-2", "Icon-2", "Label-2", "Seed-2"},
				{"e-1", "Person", "Anonymous", "Bob Smith", "TRUE", "e-2", "Person", "Anonymous", "Sally Jones", "FALSE"},
				{"e-1", "Person", "Anonymous", "Bob Smith", "TRUE", "e-3", "Address", "Location", "31 Field Drive, EH36 5PB", "FALSE"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			// Make the form to upload
			form := buildSpiderFormData(testCase.numberSteps, testCase.seedEntities)

			// Make the HTTP request
			req := httptest.NewRequest(http.MethodPost, "/spider-upload", strings.NewReader(form.Encode()))
			req.Form = form

			// Perform the upload
			w := httptest.NewRecorder()
			server.spiderUpload(w, req)

			if !testCase.jobShouldBeValid {
				assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
				return
			}

			// Check the HTTP status code and extract the GUID
			assert.Equal(t, http.StatusFound, w.Result().StatusCode)
			guid := extractSpiderGuidFromLocation(t, w.Result().Header.Get("Location"))

			// Wait for the spider jobs to finish
			waitForSpiderJobsToFinish(server.spiderRunner)

			// Request the job given its GUID
			req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/spider-job/%v", guid),
				strings.NewReader(""))
			w = httptest.NewRecorder()
			server.spiderHandleJob(w, req)

			// Check the HTTP status code
			assert.Equal(t, http.StatusOK, w.Result().StatusCode)

			if !testCase.resultsExpected {
				assert.True(t, webPageContainsText(w, guid, "No spider results"))

				// Try to download results when there are none
				req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/spider-download/%v", guid),
					strings.NewReader(""))
				w = httptest.NewRecorder()
				server.spiderHandleDownload(w, req)

				// Check the HTTP status code
				assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

				// Try to download results for a non-existent job
				req = httptest.NewRequest(http.MethodGet, "/spider-download/1234",
					strings.NewReader(""))
				w = httptest.NewRecorder()
				server.spiderHandleDownload(w, req)
				assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)

				return
			}

			assert.True(t, webPageContainsText(w, guid, "Download Excel file"))

			// Try to download the results file
			req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/spider-download/%v", guid),
				strings.NewReader(""))
			w = httptest.NewRecorder()
			server.spiderHandleDownload(w, req)

			// Check the HTTP status code
			assert.Equal(t, http.StatusOK, w.Result().StatusCode)

			disposition := w.Result().Header.Get("Content-Disposition")
			assert.Equal(t, "attachment; filename=spider-matcher-results.xlsx", disposition)

			// Save the data in the body of the response to a file
			data, err := ioutil.ReadAll(w.Body)
			assert.NoError(t, err)

			tempFilepath := server.spiderRunner.folder + "/temp.xlsx"
			assert.NoError(t, os.WriteFile(tempFilepath, data, 0644))

			// Read the excel file
			actual, err := i2chart.ReadFromExcel(tempFilepath, "Sheet1")
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedTable, actual)
		})
	}
}
