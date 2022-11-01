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

// A DocumentsCsvFile specifies the location and format of a CSV file containing documents.
type DocumentsCsvFile struct {
	Path             string            `json:"path"`             // Location of the file
	DocumentType     string            `json:"documentType"`     // Type of documents in the file
	Delimiter        string            `json:"delimiter"`        // Delimiter
	DocumentIdField  string            `json:"documentIdField"`  // Name of the field with the document ID
	FieldToAttribute map[string]string `json:"fieldToAttribute"` // Mapping of field name to attribute
}

// NewDocumentsCsvFile creates a new DocumentsCsvFile struct.
func NewDocumentsCsvFile(path string, documentType string, delimiter string,
	documentIdField string, fieldToAttribute map[string]string) DocumentsCsvFile {

	return DocumentsCsvFile{
		Path:             path,
		DocumentType:     documentType,
		Delimiter:        delimiter,
		DocumentIdField:  documentIdField,
		FieldToAttribute: fieldToAttribute,
	}
}

// DocumentsCsvFileReader reads Documents from a CSV file.
type DocumentsCsvFileReader struct {
	documentsCsvFile     DocumentsCsvFile
	csvReader            *csv.Reader
	file                 *os.File
	documentIdFieldIndex int
	attributeFieldIndex  map[string]int

	nextDocument      graphstore.Document // Next document
	hasNext           bool                // Is there another document to read?
	numberOfDocuments int                 // Number of documents parsed
	numberOfRows      int                 // Number of lines (>= number of documents + 1)
}

// A NewDocumentsCsvFileReader constructs a reader of Documents from a CSV file.
func NewDocumentsCsvFileReader(csv DocumentsCsvFile) *DocumentsCsvFileReader {
	return &DocumentsCsvFileReader{
		documentsCsvFile:  csv,
		numberOfDocuments: 0,
		numberOfRows:      0,
	}
}

// Initialise the Document reader.
func (reader *DocumentsCsvFileReader) Initialise() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Opening CSV file of documents")

	// Open the file
	var err error
	reader.file, err = os.Open(reader.documentsCsvFile.Path)
	if err != nil {
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Creating the CSV reader")

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Reading CSV file header")

	// Read the header from the file
	header, err := reader.csvReader.Read()
	if err != nil {
		return err
	}

	reader.numberOfRows += 1

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Str("documentIdField", reader.documentsCsvFile.DocumentIdField).
		Msg("Finding index of the Document ID field")

	// Find the document ID field index
	fieldToIndex, err := findIndicesOfFields(header, []string{reader.documentsCsvFile.DocumentIdField})

	if err != nil {
		return err
	}

	reader.documentIdFieldIndex = fieldToIndex[reader.documentsCsvFile.DocumentIdField]

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Creating a mapping from an attribute to a field index")

	// Create a mapping from the attribute to the field index in the CSV file
	reader.attributeFieldIndex, err = attributeToFieldIndex(header, reader.documentsCsvFile.FieldToAttribute)

	if err != nil {
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Reading the first Document")

	// Read the first record
	reader.nextDocument, reader.hasNext = reader.readRecord()

	return nil
}

// readRecord from the CSV file containing Documents.
func (reader *DocumentsCsvFileReader) readRecord() (graphstore.Document, bool) {

	recordFound := false
	var record []string
	var document graphstore.Document

	for !recordFound {
		var err error
		record, err = reader.csvReader.Read()

		if err == io.EOF {
			// End of file
			return graphstore.Document{}, false
		}

		reader.numberOfRows += 1

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Str("parseError", err.Error()).
				Msg("Line failed to parse")
			continue
		}

		// Extract the document ID
		documentId := record[reader.documentIdFieldIndex]

		// Extract the document attributes
		attributes, err := extractAttributes(record, reader.attributeFieldIndex)

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Err(err).
				Msg("Failed to extract attributes from record")
			continue
		}

		// Build the document
		document, err = graphstore.NewDocument(documentId, reader.documentsCsvFile.DocumentType,
			attributes)

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Err(err).
				Msg("Failed to build a document from record")
			continue
		}

		// Increment the number of documents parsed from the CSV file
		reader.numberOfDocuments += 1

		recordFound = true
	}

	return document, true
}

// Next Document from the file.
func (reader *DocumentsCsvFileReader) Next() (graphstore.Document, error) {

	if !reader.hasNext {
		return graphstore.Document{}, errors.New("Next() called when no next item exists")
	}

	// Get the current Document
	current := reader.nextDocument

	// Try to read the next record
	reader.nextDocument, reader.hasNext = reader.readRecord()

	if reader.hasNext && reader.numberOfRows%100000 == 0 {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("numberOfRowsRead", strconv.Itoa(reader.numberOfRows)).
			Str("numberOfDocumentsRead", strconv.Itoa(reader.numberOfDocuments)).
			Str("filepath", reader.documentsCsvFile.Path).
			Msg("Reading documents from CSV file")
	}

	return current, nil
}

// Close the documents CSV file
func (reader *DocumentsCsvFileReader) Close() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfRowsRead", strconv.Itoa(reader.numberOfRows)).
		Str("numberOfDocumentsRead", strconv.Itoa(reader.numberOfDocuments)).
		Str("filepath", reader.documentsCsvFile.Path).
		Msg("Closing CSV file")

	return reader.file.Close()
}

// ReadAll the documents from the CSV file.
func (reader *DocumentsCsvFileReader) ReadAll() ([]graphstore.Document, error) {

	// Initialise the CSV readers
	err := reader.Initialise()
	if err != nil {
		return nil, err
	}

	// Read all the documents from the file
	documents := []graphstore.Document{}

	for reader.hasNext {
		document, err := reader.Next()

		if err != nil {
			return documents, err
		}

		documents = append(documents, document)
	}

	// Close the reader
	err = reader.Close()

	return documents, err
}
