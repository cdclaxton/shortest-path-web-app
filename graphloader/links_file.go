// A link represents a connection between a document and an entity. More
// specifically that an entity with a given ID is contained within the
// document with the referenced ID.
//
// The LinksCsvFile represents the configuration for a single CSV file
// containing links.
//
// A single file can be read with a LinksCsvFileReader. This iterates through
// the file reading the links.
//
// If a line in the file contains an inconsistent number of fields, the line
// is skipped.

package graphloader

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

// LinksCsvFile represents the configuration of a single CSV file of entity-document links.
type LinksCsvFile struct {
	Path            string `json:"path"`            // Location of the file
	EntityIdField   string `json:"entityIdField"`   // Name of the field holding the entity ID
	DocumentIdField string `json:"documentIdField"` // Name of the field holding the document ID
	Delimiter       string `json:"delimiter"`       // Delimiter
}

func NewLinksCsvFile(path string, entityIdField string, documentIdField string,
	delimiter string) LinksCsvFile {

	return LinksCsvFile{
		Path:            path,
		EntityIdField:   entityIdField,
		DocumentIdField: documentIdField,
		Delimiter:       delimiter,
	}
}

// LinksCsvFileReader iterates through the CSV file producing Link structs.
type LinksCsvFileReader struct {
	linksCsvFile         LinksCsvFile
	csvReader            *csv.Reader
	file                 *os.File
	entityIdFieldIndex   int
	documentIdFieldIndex int

	nextLinks     graphstore.Link // Next link
	hasNext       bool            // Is there another link?
	numberOfLinks int             // Number of links parsed
	numberOfRows  int             // Number of lines (>= number of links + 1)
}

// NewLinksCsvFileReader from the definition of the links CSV file.
func NewLinksCsvFileReader(csv LinksCsvFile) *LinksCsvFileReader {
	return &LinksCsvFileReader{
		linksCsvFile:  csv,
		numberOfLinks: 0,
		numberOfRows:  0,
	}
}

// Initialise the links CSV file reader.
func (reader *LinksCsvFileReader) Initialise() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.linksCsvFile.Path).
		Msg("Opening CSV file of links")

	// Open the file
	var err error
	reader.file, err = os.Open(reader.linksCsvFile.Path)
	if err != nil {
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.linksCsvFile.Path).
		Msg("Creating the CSV file reader")

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.linksCsvFile.Path).
		Msg("Reading CSV file header")

	// Read the header from the file
	header, err := reader.csvReader.Read()
	if err != nil {
		return err
	}

	reader.numberOfRows += 1

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.linksCsvFile.Path).
		Msg("Finding indices of the Document ID and the Entity ID")

	// Find the entity ID and document ID field indices
	fieldToIndex, err := findIndicesOfFields(header,
		[]string{reader.linksCsvFile.EntityIdField, reader.linksCsvFile.DocumentIdField})

	if err != nil {
		return err
	}

	reader.entityIdFieldIndex = fieldToIndex[reader.linksCsvFile.EntityIdField]
	reader.documentIdFieldIndex = fieldToIndex[reader.linksCsvFile.DocumentIdField]

	// Read the first record
	reader.nextLinks, reader.hasNext = reader.readRecord()

	return nil
}

// readRecord from the CSV file.
func (reader *LinksCsvFileReader) readRecord() (graphstore.Link, bool) {

	recordFound := false
	var record []string

	for !recordFound {
		var err error
		record, err = reader.csvReader.Read()

		if err == io.EOF {
			// End of file
			return graphstore.Link{}, false
		}

		reader.numberOfRows += 1

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Err(err).
				Msg("Line failed to parse")
			continue
		}

		recordFound = true
		reader.numberOfLinks += 1
	}

	return graphstore.NewLink(record[reader.entityIdFieldIndex], record[reader.documentIdFieldIndex]),
		true
}

// Next links struct from the file.
func (reader *LinksCsvFileReader) Next() (graphstore.Link, error) {

	// Preconditions
	if !reader.hasNext {
		return graphstore.Link{}, errors.New("Next() called when no next item exists")
	}

	// Get the current Links struct
	current := reader.nextLinks

	// Try to read the next record
	reader.nextLinks, reader.hasNext = reader.readRecord()

	if reader.hasNext && reader.numberOfRows%100000 == 0 {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("filepath", reader.linksCsvFile.Path).
			Str("numberOfRowsRead", strconv.Itoa(reader.numberOfRows)).
			Str("numberOfLinksRead", strconv.Itoa(reader.numberOfLinks)).
			Msg("Reading links from CSV file")
	}

	return current, nil
}

// Close the links CSV file.
func (reader *LinksCsvFileReader) Close() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.linksCsvFile.Path).
		Str("numbeOfRowsRead", strconv.Itoa(reader.numberOfRows)).
		Str("numberOfLinksRead", strconv.Itoa(reader.numberOfLinks)).
		Msg("Closing CSV file")

	return reader.file.Close()
}

// ReadALl the links from the CSV file.
func (reader *LinksCsvFileReader) ReadAll() ([]graphstore.Link, error) {

	// Initialise the CSV readers
	err := reader.Initialise()
	if err != nil {
		return nil, err
	}

	// Read all the links from the file
	links := []graphstore.Link{}

	for reader.hasNext {
		link, err := reader.Next()

		if err != nil {
			return links, err
		}

		links = append(links, link)
	}

	// Close the reader
	err = reader.Close()

	return links, err
}
