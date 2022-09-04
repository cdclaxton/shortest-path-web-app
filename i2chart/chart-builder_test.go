package i2chart

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadI2Config(t *testing.T) {
	filepath := "./test-data/i2-config-1.json"

	config, err := readI2Config(filepath)
	assert.NoError(t, err)
	assert.NotNil(t, config)
}

func TestValidateI2Config(t *testing.T) {
	testCases := []struct {
		filepath      string
		isValid       bool
		numberReasons int
	}{
		{
			filepath:      "./test-data/i2-config-1.json",
			isValid:       true,
			numberReasons: 0,
		},
		{
			filepath:      "./test-data/i2-invalid-config-1.json",
			isValid:       false,
			numberReasons: 2,
		},
		{
			filepath:      "./test-data/i2-invalid-config-2.json",
			isValid:       false,
			numberReasons: 1,
		},
		{
			filepath:      "./test-data/i2-invalid-config-3.json",
			isValid:       false,
			numberReasons: 1,
		},
	}

	for _, testCase := range testCases {

		// Read the JSON file
		config, err := readI2Config(testCase.filepath)
		assert.NoError(t, err)
		assert.NotNil(t, config)

		// Validate the config
		valid, reasons := validateI2Config(*config)
		assert.Equal(t, testCase.isValid, valid)
		assert.Equal(t, testCase.numberReasons, len(reasons))
	}
}

func TestHeader(t *testing.T) {
	testCases := []struct {
		columns  []string
		expected []string
	}{
		{
			columns:  []string{"Name"},
			expected: []string{"Entity-Name-1", "Entity-Name-2", "Link"},
		},
		{
			columns: []string{"Name", "Dob"},
			expected: []string{"Entity-Name-1", "Entity-Dob-1",
				"Entity-Name-2", "Entity-Dob-2", "Link"},
		},
	}

	for _, testCase := range testCases {
		actual := header(testCase.columns)
		assert.Equal(t, testCase.expected, actual)
	}
}
