package loader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindIndicesOfFields(t *testing.T) {

	testCases := []struct {
		header        []string
		fields        []string
		expected      map[string]int
		errorExpected bool
	}{
		{
			header:        []string{"a"},
			fields:        []string{"a"},
			expected:      map[string]int{"a": 0},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: false,
		},
		{
			header:        []string{"b", "a"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 1, "b": 0},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b", "c"},
			fields:        []string{"a", "b"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: false,
		},
		{
			header:        []string{"a", "b", "c"},
			fields:        []string{"a", "b", "d"},
			expected:      map[string]int{"a": 0, "b": 1},
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := findIndicesOfFields(testCase.header, testCase.fields)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestAttributeToFieldIndex(t *testing.T) {
	testCases := []struct {
		header           []string
		fieldToAttribute map[string]string
		expected         map[string]int
		errorExpected    bool
	}{
		{
			header:           []string{"name"},
			fieldToAttribute: map[string]string{"name": "Forename"},
			expected:         map[string]int{"Forename": 0},
			errorExpected:    false,
		},
		{
			header: []string{"first", "last"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected: map[string]int{
				"Forename": 0,
				"Surname":  1},
			errorExpected: false,
		},
		{
			header: []string{"last", "first"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected: map[string]int{
				"Forename": 1,
				"Surname":  0},
			errorExpected: false,
		},
		{
			header: []string{"last"},
			fieldToAttribute: map[string]string{
				"first": "Forename",
				"last":  "Surname"},
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := attributeToFieldIndex(testCase.header, testCase.fieldToAttribute)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}

func TestExtractAttributes(t *testing.T) {
	testCases := []struct {
		header                []string
		attributeTofieldIndex map[string]int
		expected              map[string]string
		errorExpected         bool
	}{
		{
			header: []string{"Bob"},
			attributeTofieldIndex: map[string]int{
				"Forename": 0},
			expected: map[string]string{
				"Forename": "Bob",
			},
			errorExpected: false,
		},
		{
			header: []string{"Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 0,
				"Surname":  1},
			expected: map[string]string{
				"Forename": "Bob",
				"Surname":  "Smith",
			},
			errorExpected: false,
		},
		{
			header: []string{"23", "Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 1,
				"Surname":  2,
				"Age":      0},
			expected: map[string]string{
				"Age":      "23",
				"Forename": "Bob",
				"Surname":  "Smith",
			},
			errorExpected: false,
		},
		{
			header: []string{"Bob", "Smith"},
			attributeTofieldIndex: map[string]int{
				"Forename": 1,
				"Surname":  2,
				"Age":      0},
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := extractAttributes(testCase.header, testCase.attributeTofieldIndex)

		if err != nil {
			assert.True(t, testCase.errorExpected)
		} else {
			assert.False(t, testCase.errorExpected)
		}

		assert.Equal(t, testCase.expected, actual)
	}
}
