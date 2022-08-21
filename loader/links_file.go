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

package loader

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/rs/zerolog/log"
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

	nextLinks graphstore.Link
	hasNext   bool
}

// NewLinksCsvFileReader from the definition of the links CSV file.
func NewLinksCsvFileReader(csv LinksCsvFile) *LinksCsvFileReader {
	return &LinksCsvFileReader{
		linksCsvFile: csv,
	}
}

// Initialise the links CSV file reader.
func (reader *LinksCsvFileReader) Initialise() error {

	// Open the file
	var err error
	reader.file, err = os.Open(reader.linksCsvFile.Path)
	if err != nil {
		return err
	}

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)

	// Read the header from the file
	header, err := reader.csvReader.Read()

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

		if err != nil {
			log.Warn().Str("Component", "LinksCsvFileReader").
				Str("Parse error", err.Error()).
				Msg("Line failed to parse")
			continue
		}

		recordFound = true
	}

	return graphstore.NewLink(record[reader.entityIdFieldIndex], record[reader.documentIdFieldIndex]),
		true
}

// Next links struct from the file.
func (reader *LinksCsvFileReader) Next() (graphstore.Link, error) {

	// Preconditions
	if !reader.hasNext {
		return graphstore.Link{}, fmt.Errorf("Next() called when no next item exists")
	}

	// Get the current Links struct
	current := reader.nextLinks

	// Try to read the next record
	reader.nextLinks, reader.hasNext = reader.readRecord()

	return current, nil
}

// Close the links CSV file.
func (reader *LinksCsvFileReader) Close() error {
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
