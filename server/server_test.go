package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestNewJobServer(t *testing.T) {
	runner := makeJobRunner(t)
	server, err := NewJobServer(runner)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}
