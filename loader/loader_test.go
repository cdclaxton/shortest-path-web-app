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
