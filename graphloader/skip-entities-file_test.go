package graphloader

import (
	"testing"

	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/stretchr/testify/assert"
)

func TestReadSkipEntities(t *testing.T) {
	testCases := []struct {
		filepath         string
		expectedEntities *set.Set[string]
	}{
		{
			filepath:         "./test-data/skip_1.txt",
			expectedEntities: set.NewPopulatedSet("e-1", "e-2", "e-3", "e-4"),
		},
		{
			filepath:         "./test-data/skip_2.txt",
			expectedEntities: set.NewSet[string](),
		},
	}

	for _, testCase := range testCases {
		actualEntities, err := ReadSkipEntities(testCase.filepath)
		assert.NoError(t, err)
		assert.True(t, testCase.expectedEntities.Equal(actualEntities))
	}
}
