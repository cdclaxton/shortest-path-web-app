package filedetector

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/logging"
)

const componentName = "fileDetector"

var (
	ErrSignatureFileDoesNotExist = errors.New("signature file does not exist")
	ErrFileSignatureIsNil        = errors.New("file signature info is nil")
	ErrEmptyFilepath             = errors.New("file signature file is empty")
)

type FileSignatures map[string]string

type FileSignatureInfo struct {
	Signatures  FileSignatures `json:"signatures"`  // Signatures of each file
	DateCreated time.Time      `json:"dateCreated"` // Date and time the signature was created
}

// FilesChanged detects whether the a list of files have changed based on their file hash.
func FilesChanged(filepaths []string, signatureFilepath string) (bool, *FileSignatureInfo, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Int("numFiles", len(filepaths)).
		Str("signatureFilePath", signatureFilepath).
		Msg("Detecting file changes")

	// Try to read the signature file from disk
	hasPrevious := true
	previous, err := readFileSignatures(signatureFilepath)
	if err == ErrSignatureFileDoesNotExist || err == ErrEmptyFilepath {

		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("signatureFilePath", signatureFilepath).
			Msg("Signature file doesn't exist")

		hasPrevious = false
	} else if err != nil {

		logging.Logger.Warn().
			Str(logging.ComponentField, componentName).
			Str("signatureFilePath", signatureFilepath).
			Msg("Failed to read signature file")

		return false, nil, err
	}

	// Generate signatures
	sigInfo, err := generateSignaturesOfFiles(filepaths)
	if err != nil {
		return false, nil, err
	}

	// If there isn't a previous set of file signatures, return the new signatures
	if !hasPrevious {
		return true, sigInfo, nil
	}

	// Check to see if the current signatures are identical to the previous
	if signaturesSame(sigInfo.Signatures, previous.Signatures) {
		return false, nil, nil
	}

	return true, sigInfo, nil
}

// FilesChangedInFolder detects whether the files in a folder have changed based on their file hash.
//
// If the files have changed, the function returns the file signature information for later use
// (e.g. to write it to file using WriteFileSignatures).
func FilesChangedInFolder(folder string, signatureFilepath string) (bool, *FileSignatureInfo, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("folder", folder).
		Str("signatureFilePath", signatureFilepath).
		Msg("Detecting file changes in folder")

	// Try to read the signature file from disk
	hasPrevious := true
	previous, err := readFileSignatures(signatureFilepath)
	if err == ErrSignatureFileDoesNotExist {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("signatureFilePath", signatureFilepath).
			Msg("Signature file doesn't exist")

		hasPrevious = false
	} else if err != nil {
		logging.Logger.Warn().
			Str(logging.ComponentField, componentName).
			Str("signatureFilePath", signatureFilepath).
			Msg("Failed to read signature file")

		return false, nil, err
	}

	// Generate signatures
	sigInfo, err := generateSignatures(folder)
	if err != nil {
		return false, nil, err
	}

	// If there isn't a previous set of file signatures, return the new signatures
	if !hasPrevious {
		return true, sigInfo, nil
	}

	// Check to see if the current signatures are identical to the previous
	if signaturesSame(sigInfo.Signatures, previous.Signatures) {
		return false, nil, nil
	}

	return true, sigInfo, nil
}

// WriteFileSignatures writes the file signature information to a JSON file.
func WriteFileSignatures(sigInfo *FileSignatureInfo, filepath string) error {

	if sigInfo == nil {
		return ErrFileSignatureIsNil
	}

	if len(filepath) == 0 {
		return ErrEmptyFilepath
	}

	// Marhsall the signature information to JSON
	data, err := json.Marshal(sigInfo)
	if err != nil {
		return err
	}

	// Write the JSON to file
	return os.WriteFile(filepath, data, 0644)
}

// readFileSignatures reads the file signature information from a JSON file.
func readFileSignatures(filepath string) (*FileSignatureInfo, error) {

	if len(filepath) == 0 {
		return nil, ErrEmptyFilepath
	}

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrSignatureFileDoesNotExist
		}
		return nil, err
	}

	defer file.Close()

	// Read the JSON into a byte array
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Unmarshall the data
	var fileSignatureInfo FileSignatureInfo
	err = json.Unmarshal(content, &fileSignatureInfo)

	if err != nil {
		return nil, err
	}

	return &fileSignatureInfo, nil
}

func generateSignaturesOfFiles(filepaths []string) (*FileSignatureInfo, error) {

	sig := FileSignatures{}

	var err error
	for idx, filepath := range filepaths {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("filepath", filepath).
			Int("fileNumber", idx+1).
			Int("numFiles", len(filepaths)).
			Msg("Generating file signature")

		sig[filepath], err = hashFile(filepath)
		if err != nil {
			return nil, err
		}
	}

	return &FileSignatureInfo{
		Signatures:  sig,
		DateCreated: time.Now(),
	}, nil
}

// generateSignatures of all files in the folder.
func generateSignatures(folder string) (*FileSignatureInfo, error) {

	entries, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	sig := FileSignatures{}

	// Walk through each file in the folder
	for _, e := range entries {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("folder", folder).
			Str("filename", e.Name()).
			Msg("Generating file signature")

		filepath := path.Join(folder, e.Name())
		sig[e.Name()], err = hashFile(filepath)
		if err != nil {
			return nil, err
		}
	}

	return &FileSignatureInfo{
		Signatures:  sig,
		DateCreated: time.Now(),
	}, nil
}

// hashFile generates a SHA-256 hash of the file.
func hashFile(filepath string) (string, error) {

	// Open the file
	f, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Generate a SHA-256 hash
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	// Return a string representation of the hash
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// signaturesSame returns true if the two file signatures are identical.
func signaturesSame(current FileSignatures, previous FileSignatures) bool {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Checking if file signatures are the same")

	if len(current) != len(previous) {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Int("current", len(current)).
			Int("previous", len(previous)).
			Msg("Different number of files")
		return false
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Same number of files")

	for filename, hash1 := range current {
		hash2, found := previous[filename]
		if !found {
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("filename", filename).
				Msg("File in current signature missing from previous signature")
			return false
		}

		if hash1 != hash2 {
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("filename", filename).
				Str("currentHash", hash1).
				Str("previousHash", hash2).
				Msg("File has different signatures")
			return false
		}
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Signatures are identical")

	return true
}
