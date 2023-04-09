package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSpiderJobConfiguration(t *testing.T) {
	testCases := []struct {
		numberSteps   int
		seedEntities  []string
		expected      *SpiderJobConfiguration
		errorExpected bool
	}{
		{
			numberSteps:   -1,
			seedEntities:  []string{"e-1"},
			expected:      nil,
			errorExpected: true,
		},
		{
			numberSteps:  0,
			seedEntities: []string{"e-1"},
			expected: &SpiderJobConfiguration{
				NumberSteps:  0,
				SeedEntities: []string{"e-1"},
			},
			errorExpected: false,
		},
		{
			numberSteps:  0,
			seedEntities: []string{"e-1", "e-2"},
			expected: &SpiderJobConfiguration{
				NumberSteps:  0,
				SeedEntities: []string{"e-1", "e-2"},
			},
			errorExpected: false,
		},
		{
			numberSteps:   0,
			seedEntities:  []string{"e-1", ""},
			expected:      nil,
			errorExpected: true,
		},
		{
			numberSteps:   0,
			seedEntities:  []string{"e-1", " "},
			expected:      nil,
			errorExpected: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := NewSpiderJobConfiguration(testCase.numberSteps, testCase.seedEntities)

		assert.Equal(t, testCase.expected, actual)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestNewSpiderJob(t *testing.T) {
	testCases := []struct {
		conf          *SpiderJobConfiguration
		expected      SpiderJob
		errorExpected bool
	}{
		{
			// Invalid config
			conf: &SpiderJobConfiguration{
				NumberSteps:  -1,
				SeedEntities: []string{"e-1"},
			},
			errorExpected: true,
		},
		{
			// Invalid config
			conf: &SpiderJobConfiguration{
				NumberSteps:  1,
				SeedEntities: []string{"e-1", ""},
			},
			errorExpected: true,
		},
		{
			conf: &SpiderJobConfiguration{
				NumberSteps:  1,
				SeedEntities: []string{"e-1", "e-2"},
			},
			errorExpected: false,
		},
	}

	for _, testCase := range testCases {
		actual, err := NewSpiderJob(testCase.conf)

		if testCase.errorExpected {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.True(t, len(actual.GUID) > 0)
			assert.Equal(t, testCase.conf, actual.Configuration)
			assert.Equal(t, NotStarted, actual.Progress.State)
		}
	}
}
