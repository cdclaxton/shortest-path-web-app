package i2chart

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindKeywords(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{
			input:    "no keywords",
			expected: []string{},
		},
		{
			input:    "<keyword>",
			expected: []string{"<keyword>"},
		},
		{
			input:    "This is a <keyword>",
			expected: []string{"<keyword>"},
		},
		{
			input:    "This is a <keyword>, and this is <keyword2>",
			expected: []string{"<keyword>", "<keyword2>"},
		},
		{
			input:    "This is a <keyword>, <keyword2> / <keyword3>",
			expected: []string{"<keyword>", "<keyword2>", "<keyword3>"},
		},
	}

	for _, testCase := range testCases {
		actual, err := findKeywords(testCase.input)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestSubstitute(t *testing.T) {
	testCases := []struct {
		format         string
		keywordToValue map[string]string
		missing        string
		expectedError  bool
		expectedResult string
	}{
		{
			format: "No substitution required",
			keywordToValue: map[string]string{
				"id": "1234",
			},
			missing:        "Missing",
			expectedError:  false,
			expectedResult: "No substitution required",
		},
		{
			format: "<id>",
			keywordToValue: map[string]string{
				"id": "1234",
			},
			missing:        "Missing",
			expectedError:  false,
			expectedResult: "1234",
		},
		{
			format: "No substitution required",
			keywordToValue: map[string]string{
				"id": "1234",
			},
			missing:        "Missing",
			expectedError:  false,
			expectedResult: "No substitution required",
		},
		{
			format: "<name> has id <id>",
			keywordToValue: map[string]string{
				"id":   "1234",
				"name": "Bob Smith",
			},
			missing:        "Missing",
			expectedError:  false,
			expectedResult: "Bob Smith has id 1234",
		},
		{
			format: "<fullname> has id <id>",
			keywordToValue: map[string]string{
				"id": "1234",
			},
			missing:        "Missing",
			expectedError:  false,
			expectedResult: "Missing has id 1234",
		},
		{
			format: "id <id>",
			keywordToValue: map[string]string{
				"<id>": "1234", // this is invalid
			},
			missing:        "Missing",
			expectedError:  true,
			expectedResult: "",
		},
		{
			format: "id <id>",
			keywordToValue: map[string]string{
				"id": "1234",
				"":   "A", // this is invalid
			},
			missing:        "Missing",
			expectedError:  true,
			expectedResult: "",
		},
	}

	for _, testCase := range testCases {
		actual, err := Substitute(testCase.format, testCase.keywordToValue, testCase.missing)
		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedResult, actual)
	}
}
