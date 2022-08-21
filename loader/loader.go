package loader

import (
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
)

type GraphStoreLoaderFromCsv struct {
	graphStore    graphstore.BipartiteGraphStore
	entityFiles   []EntitiesCsvFile
	documentFiles []DocumentsCsvFile
	linkFiles     []LinksCsvFile
}

func NewGraphStoreLoaderFromCsv(graphStore graphstore.BipartiteGraphStore,
	entityFiles []EntitiesCsvFile,
	documentFiles []DocumentsCsvFile,
	linkFiles []LinksCsvFile) *GraphStoreLoaderFromCsv {

	return &GraphStoreLoaderFromCsv{
		graphStore:    graphStore,
		entityFiles:   entityFiles,
		documentFiles: documentFiles,
		linkFiles:     linkFiles,
	}
}

// loadEntitiesFromFile into the graph store from a CSV file.
func (loader *GraphStoreLoaderFromCsv) loadEntitiesFromFile(file EntitiesCsvFile) error {

	// Create an entities CSV file reader
	reader := NewEntitiesCsvFileReader(file)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has entities to read, add the entities to the graph store
	for reader.hasNext {
		entity, err := reader.Next()

		if err != nil {
			return err
		}

		if err := loader.graphStore.AddEntity(entity); err != nil {
			return err
		}
	}

	return reader.Close()
}

// loadEntities from each of the CSV files.
func (loader *GraphStoreLoaderFromCsv) loadEntities() error {

	for _, file := range loader.entityFiles {
		err := loader.loadEntitiesFromFile(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// loadEntitiesFromFile into the graph store from a CSV file.
func (loader *GraphStoreLoaderFromCsv) loadDocumentsFromFile(file DocumentsCsvFile) error {

	// Create a documents CSV file reader
	reader := NewDocumentsCsvFileReader(file)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has documents to read, add the documents to the graph store
	for reader.hasNext {
		document, err := reader.Next()

		if err != nil {
			return err
		}

		if err := loader.graphStore.AddDocument(document); err != nil {
			return err
		}
	}

	return reader.Close()
}

// loadDocuments from each of the CSV files.
func (loader *GraphStoreLoaderFromCsv) loadDocuments() error {

	for _, file := range loader.documentFiles {
		err := loader.loadDocumentsFromFile(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// loadLinksFromFile into the graph store from a CSV file.
func (loader *GraphStoreLoaderFromCsv) loadLinksFromFile(file LinksCsvFile) error {

	// Create a links CSV file reader
	reader := NewLinksCsvFileReader(file)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has links to read, add the links to the graph store
	for reader.hasNext {
		link, err := reader.Next()

		if err != nil {
			return err
		}

		if err := loader.graphStore.AddLink(link); err != nil {
			return err
		}
	}

	return reader.Close()
}

// loadLinks from each of the CSV files.
func (loader *GraphStoreLoaderFromCsv) loadLinks() error {

	for _, file := range loader.linkFiles {
		err := loader.loadLinksFromFile(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// Load the graph store from the CSV files.
func (loader *GraphStoreLoaderFromCsv) Load() error {

	// Load the entities
	err := loader.loadEntities()
	if err != nil {
		return err
	}

	// Load the documents
	err = loader.loadDocuments()
	if err != nil {
		return err
	}

	// Load the links
	err = loader.loadLinks()
	return err
}

// findIndicesOfFields returns a mapping of the field name to index.
func findIndicesOfFields(header []string, fields []string) (map[string]int, error) {

	// Map containing all the fields present in the header
	allFields := map[string]int{}

	for idx, name := range header {
		allFields[name] = idx
	}

	// Create a mapping for the fields that are required
	needed := map[string]int{}
	missingFields := []string{}

	for _, field := range fields {
		index, found := allFields[field]
		if !found {
			missingFields = append(missingFields, field)
		} else {
			needed[field] = index
		}
	}

	// Return an error message if any of the fields are missing
	if len(missingFields) != 0 {
		return needed, fmt.Errorf("Header has missing field(s): %v",
			strings.Join(missingFields, ","))
	}

	return needed, nil
}

// attributeToFieldIndex creates a mapping from the attribute name to the field index.
func attributeToFieldIndex(header []string, fieldToAttribute map[string]string) (
	map[string]int, error) {

	// Slice of field names
	fieldNames := []string{}
	for field := range fieldToAttribute {
		fieldNames = append(fieldNames, field)
	}

	// Find the attribute field indices
	fieldToIndex, err := findIndicesOfFields(header, fieldNames)

	if err != nil {
		return nil, err
	}

	// Map of attribute name to field index
	attributeFieldIndex := map[string]int{}

	for _, field := range fieldNames {
		attributeName := fieldToAttribute[field]
		attributeFieldIndex[attributeName] = fieldToIndex[field]
	}

	return attributeFieldIndex, nil
}

// extractAttributes from a row of data given the mapping from the attribute name to field index.
func extractAttributes(row []string, attributeToFieldIndex map[string]int) (
	map[string]string, error) {

	// Map of attribute name to its value
	attributes := map[string]string{}

	for attributeName, fieldIndex := range attributeToFieldIndex {

		// Check the field index is valid
		if fieldIndex < 0 || fieldIndex >= len(row) {
			return nil, fmt.Errorf("Invalid field index: %v", fieldIndex)
		}

		attributes[attributeName] = row[fieldIndex]
	}

	return attributes, nil
}
