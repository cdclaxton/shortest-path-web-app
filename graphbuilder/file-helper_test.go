package graphbuilder

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClearFolder(t *testing.T) {

	// Make a temporary folder
	tempFolder, err := ioutil.TempDir("", "folder-test")
	assert.NoError(t, err)

	// Create a file within the folder
	file, err := os.Create(filepath.Join(tempFolder, "test.txt"))
	assert.NoError(t, err)
	file.Close()

	// Create a sub-folder
	os.Mkdir(filepath.Join(tempFolder, "f1"), 0700)

	// Create a file within the sub-folder
	file, err = os.Create(filepath.Join(tempFolder, "f1", "test2.txt"))
	assert.NoError(t, err)
	file.Close()

	// Check the folder is not empty
	empty, err := isFolderEmpty(tempFolder)
	assert.NoError(t, err)
	assert.False(t, empty)

	// Try to delete the files in the folder
	assert.NoError(t, clearFolder(tempFolder))

	// Check the temp folder is empty
	empty, err = isFolderEmpty(tempFolder)
	assert.NoError(t, err)
	assert.True(t, empty)

	// Delete the temp folder
	assert.NoError(t, os.RemoveAll(tempFolder))
}
