package main

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
			datasetIndex:  -1,
			name1:         "Dataset 1",
			entityIds1:    "1234",
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

		actual, err := parseDataset(req, testCase.datasetIndex)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}
