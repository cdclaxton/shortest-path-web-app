package filedetector

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFilesChanged(t *testing.T) {

	testCases := []struct {
		description   string   // Description for the test
		hasPrevious   bool     // Should there be a previous signature file?
		previousFiles []string // Previous filepaths
		currentFiles  []string // Current filepaths
		change        bool     // Should a change be expected?
	}{
		{
			description:   "no previous files, one current file",
			hasPrevious:   false,
			previousFiles: []string{},
			currentFiles: []string{
				"./test-data/test-1/a.txt",
			},
			change: true,
		},
		{
			description: "one previous file, one current file, no change",
			hasPrevious: true,
			previousFiles: []string{
				"./test-data/test-1/a.txt",
			},
			currentFiles: []string{
				"./test-data/test-1/a.txt",
			},
			change: false,
		},
		{
			description: "one previous file, one current file, one (filepath) change",
			hasPrevious: true,
			previousFiles: []string{
				"./test-data/test-1/a.txt",
			},
			currentFiles: []string{
				"./test-data/test-2/a.txt",
			},
			change: true,
		},
		{
			description: "two previous files, two current files, no change",
			hasPrevious: true,
			previousFiles: []string{
				"./test-data/test-3/a.txt",
				"./test-data/test-3/b.txt",
			},
			currentFiles: []string{
				"./test-data/test-3/a.txt",
				"./test-data/test-3/b.txt",
			},
			change: false,
		},
		{
			description: "two previous files, one current files, change",
			hasPrevious: true,
			previousFiles: []string{
				"./test-data/test-3/a.txt",
				"./test-data/test-3/b.txt",
			},
			currentFiles: []string{
				"./test-data/test-3/a.txt",
			},
			change: true,
		},
	}

	// Create a temp folder to write the signature file to
	signatureFolder, err := ioutil.TempDir("", "fileSignature")
	assert.NoError(t, err)
	defer os.RemoveAll(signatureFolder)

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			previousSignatureFilepath := path.Join(signatureFolder, "signature.json")

			if testCase.hasPrevious {
				// Generate the previous file signatures
				previous, err := generateSignaturesOfFiles(testCase.previousFiles)
				assert.NoError(t, err)
				assert.NotNil(t, previous)

				// Write the file signature to file
				err = WriteFileSignatures(previous, previousSignatureFilepath)
				assert.NoError(t, err)
			}

			// Determine whether a change occurred
			actual, sig, err := FilesChanged(testCase.currentFiles, previousSignatureFilepath)
			assert.NoError(t, err)
			assert.Equal(t, testCase.change, actual)

			if testCase.change {
				assert.NotNil(t, sig)
			} else {
				assert.Nil(t, sig)
			}
		})
	}
}

func TestFilesChangedInFolder(t *testing.T) {

	testCases := []struct {
		description    string // Description for the test
		hasPrevious    bool   // Should there be a previous signature file?
		previousFolder string // Folder used to generate previous signatures
		currentFolder  string // Folder used to generate current signatures
		change         bool   // Should a change be expected?
	}{
		{
			description:    "no previous files",
			hasPrevious:    false,
			previousFolder: "",
			currentFolder:  "test-1",
			change:         true,
		},
		{
			description:    "same file and content",
			hasPrevious:    true,
			previousFolder: "test-1",
			currentFolder:  "test-1",
			change:         false,
		},
		{
			description:    "same file, different content",
			hasPrevious:    true,
			previousFolder: "test-1",
			currentFolder:  "test-2",
			change:         true,
		},
		{
			description:    "different files",
			hasPrevious:    true,
			previousFolder: "test-1",
			currentFolder:  "test-3",
			change:         true,
		},
		{
			description:    "same files",
			hasPrevious:    true,
			previousFolder: "test-3",
			currentFolder:  "test-3",
			change:         false,
		},
	}

	// Create a temp folder to write the signature file to
	signatureFolder, err := ioutil.TempDir("", "fileSignature")
	assert.NoError(t, err)
	defer os.RemoveAll(signatureFolder)

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			previousSignatureFilepath := path.Join(signatureFolder, "signature.json")

			if testCase.hasPrevious {
				// Generate the previous file signatures
				previous, err := generateSignatures(path.Join("./test-data/", testCase.previousFolder))
				assert.NoError(t, err)
				assert.NotNil(t, previous)

				// Write the file signature to file
				err = WriteFileSignatures(previous, previousSignatureFilepath)
				assert.NoError(t, err)
			}

			// Determine whether a change occurred
			current := path.Join("./test-data/", testCase.currentFolder)
			actual, sig, err := FilesChangedInFolder(current, previousSignatureFilepath)
			assert.NoError(t, err)
			assert.Equal(t, testCase.change, actual)

			if testCase.change {
				assert.NotNil(t, sig)
			} else {
				assert.Nil(t, sig)
			}
		})
	}
}

func TestSignaturesSame(t *testing.T) {
	testCases := []struct {
		sig1     FileSignatures
		sig2     FileSignatures
		expected bool
	}{
		{
			sig1: FileSignatures{
				"a": "100",
			},
			sig2: FileSignatures{
				"a": "100",
			},
			expected: true,
		},
		{
			sig1: FileSignatures{
				"a": "100",
			},
			sig2: FileSignatures{
				"a": "300",
			},
			expected: false,
		},
		{
			sig1: FileSignatures{
				"a": "100",
			},
			sig2: FileSignatures{
				"a": "100",
				"b": "200",
			},
			expected: false,
		},
		{
			sig1: FileSignatures{
				"a": "100",
				"b": "200",
			},
			sig2: FileSignatures{
				"a": "100",
				"b": "200",
			},
			expected: true,
		},
		{
			sig1: FileSignatures{
				"a": "100",
				"b": "500",
			},
			sig2: FileSignatures{
				"a": "100",
				"b": "200",
			},
			expected: false,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("Test %d", idx), func(t *testing.T) {
			actual := signaturesSame(testCase.sig1, testCase.sig2)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestGenerateSignatures(t *testing.T) {
	testCases := []struct {
		folder   string
		expected FileSignatures
	}{
		{
			folder: "./test-data/test-1",
			expected: FileSignatures{
				"a.txt": "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.folder, func(t *testing.T) {
			actual, err := generateSignatures(testCase.folder)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, actual.Signatures)
		})
	}
}

func TestReadWriteFileSignatures(t *testing.T) {

	// Create a temp folder to write the signature file to
	folder, err := ioutil.TempDir("", "fileSignature")
	assert.NoError(t, err)

	defer os.RemoveAll(folder)

	sig := FileSignatureInfo{
		Signatures: FileSignatures{
			"a.txt": "100",
			"b.txt": "200",
		},
		DateCreated: time.Now(),
	}

	// File path for the signature file
	filepath := path.Join(folder, "signature.json")

	// Write the signature file to disk
	err = WriteFileSignatures(&sig, filepath)
	assert.NoError(t, err)

	// Read the signature file from disk
	sig2, err := readFileSignatures(filepath)
	assert.NoError(t, err)
	assert.Equal(t, sig.Signatures, sig2.Signatures)
}

func TestReadFileSignaturesFileDoesNotExist(t *testing.T) {
	filepath := "./test-data/signature.json"
	sig, err := readFileSignatures(filepath)
	assert.Nil(t, sig)
	assert.ErrorIs(t, err, ErrSignatureFileDoesNotExist)
}
