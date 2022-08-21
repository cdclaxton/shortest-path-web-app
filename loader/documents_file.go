package loader

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/rs/zerolog/log"
)

// A DocumentsCsvFile specifies the location and format of a CSV file containing documents.
type DocumentsCsvFile struct {
	Path             string            `json:"path"`             // Location of the file
	DocumentType     string            `json:"documentType"`     // Type of documents in the file
	Delimiter        string            `json:"delimiter"`        // Delimiter
	DocumentIdField  string            `json:"documentIdField"`  // Name of the field with the document ID
	FieldToAttribute map[string]string `json:"fieldToAttribute"` // Mapping of field name to attribute
}

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

type DocumentsCsvFileReader struct {
	documentsCsvFile     DocumentsCsvFile
	csvReader            *csv.Reader
	file                 *os.File
	documentIdFieldIndex int
	attributeFieldIndex  map[string]int

	nextDocument graphstore.Document
	hasNext      bool
}

func NewDocumentsCsvFileReader(csv DocumentsCsvFile) *DocumentsCsvFileReader {
	return &DocumentsCsvFileReader{
		documentsCsvFile: csv,
	}
}

func (reader *DocumentsCsvFileReader) Initialise() error {

	// Open the file
	var err error
	reader.file, err = os.Open(reader.documentsCsvFile.Path)
	if err != nil {
		return err
	}

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)

	// Read the header from the file
	header, err := reader.csvReader.Read()

	// Find the document ID field index
	fieldToIndex, err := findIndicesOfFields(header, []string{reader.documentsCsvFile.DocumentIdField})

	if err != nil {
		return err
	}

	reader.documentIdFieldIndex = fieldToIndex[reader.documentsCsvFile.DocumentIdField]

	// Create a mapping from the attribute to the field index in the CSV file
	reader.attributeFieldIndex, err = attributeToFieldIndex(header, reader.documentsCsvFile.FieldToAttribute)

	if err != nil {
		return err
	}

	// Read the first record
	reader.nextDocument, reader.hasNext = reader.readRecord()

	return nil
}

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

		if err != nil {
			log.Warn().Str("Component", "DocumentsCsvFileReader").
				Str("Parse error", err.Error()).
				Msg("Line failed to parse")
			continue
		}

		// Extract the document ID
		documentId := record[reader.documentIdFieldIndex]

		// Extract the document attributes
		attributes, err := extractAttributes(record, reader.attributeFieldIndex)

		if err != nil {
			log.Warn().Str("Component", "DocumentsCsvFileReader").
				Str("Error", err.Error()).
				Msg("Failed to extract attributes from record")
			continue
		}

		// Build the document
		document, err = graphstore.NewDocument(documentId, reader.documentsCsvFile.DocumentType,
			attributes)

		if err != nil {
			log.Warn().Str("Component", "DocumentsCsvFileReader").
				Str("Error", err.Error()).
				Msg("Failed to build a document from record")
			continue
		}

		recordFound = true
	}

	return document, true
}

// Next Document from the file.
func (reader *DocumentsCsvFileReader) Next() (graphstore.Document, error) {

	if !reader.hasNext {
		return graphstore.Document{}, fmt.Errorf("Next() called when no next item exists")
	}

	// Get the current Document
	current := reader.nextDocument

	// Try to read the next record
	reader.nextDocument, reader.hasNext = reader.readRecord()

	return current, nil
}

// Close the documents CSV file
func (reader *DocumentsCsvFileReader) Close() error {
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
