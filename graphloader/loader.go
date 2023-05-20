package graphloader

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

const componentName = "graphLoader"

// A GraphStoreLoaderFromCsv loads a bipartite graph store from entity, document and link CSV files.
type GraphStoreLoaderFromCsv struct {
	graphStore         graphstore.BipartiteGraphStore
	entityFiles        []EntitiesCsvFile
	documentFiles      []DocumentsCsvFile
	linkFiles          []LinksCsvFile
	ignoreInvalidLinks bool // Ignore links that cannot be created, e.g. due to missing entity or document
}

// NewGraphStoreLoaderFromCsv constructs a graph store loader that reads CSV files.
func NewGraphStoreLoaderFromCsv(graphStore graphstore.BipartiteGraphStore,
	entityFiles []EntitiesCsvFile,
	documentFiles []DocumentsCsvFile,
	linkFiles []LinksCsvFile,
	ignoreInvalidLinks bool) *GraphStoreLoaderFromCsv {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfEntityFiles", strconv.Itoa(len(entityFiles))).
		Str("numberOfDocumentFiles", strconv.Itoa(len(documentFiles))).
		Str("numberOfLinksFiles", strconv.Itoa(len(linkFiles))).
		Str("ignoreInvalidLinks", strconv.FormatBool(ignoreInvalidLinks)).
		Msg("Creating a bipartite graph store loader")

	return &GraphStoreLoaderFromCsv{
		graphStore:         graphStore,
		entityFiles:        entityFiles,
		documentFiles:      documentFiles,
		linkFiles:          linkFiles,
		ignoreInvalidLinks: ignoreInvalidLinks,
	}
}

// loadEntitiesFromFile into the graph store from a CSV file.
func (loader *GraphStoreLoaderFromCsv) loadEntitiesFromFile(file EntitiesCsvFile) error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", file.Path).
		Msg("Reading entities CSV file")

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

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", file.Path).
		Msg("Reading documents CSV file")

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

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", file.Path).
		Msg("Reading links CSV file")

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

		// Try to add the link
		err = loader.graphStore.AddLink(link)

		// If there is an error, handle it if required
		if err != nil {
			if !loader.ignoreInvalidLinks {
				return err
			} else {
				if err != graphstore.ErrEntityNotFound && err != graphstore.ErrDocumentNotFound {
					return err
				}

				logging.Logger.Info().
					Str(logging.ComponentField, componentName).
					Str("filepath", file.Path).
					Str("entityId", link.EntityId).
					Str("documentId", link.DocumentId).
					Str("message", err.Error()).
					Msg("Gracefully handling error with link")
			}
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

	// Loading of entities and documents can be performed concurrently
	errEntitiesChan := make(chan error)
	errDocumentsChan := make(chan error)

	go func() {
		errEntitiesChan <- loader.loadEntities()
	}()

	go func() {
		errDocumentsChan <- loader.loadDocuments()
	}()

	// Return the first error
	errEntities := <-errEntitiesChan
	errDocuments := <-errDocumentsChan

	if errEntities != nil {
		return errEntities
	}

	if errDocuments != nil {
		return errDocuments
	}

	// Load the links
	return loader.loadLinks()
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
		return needed, fmt.Errorf("header has missing field(s): %v",
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
			return nil, fmt.Errorf("invalid field index: %v", fieldIndex)
		}

		attributes[attributeName] = row[fieldIndex]
	}

	return attributes, nil
}

var (
	ErrEmptyDelimiter   = errors.New("empty delimiter")
	ErrInvalidDelimiter = errors.New("invalid delimiter")
)

// parseDelimiter to use when reading CSV files.
func parseDelimiter(delimiter string) (rune, error) {

	// Preconditions
	if len(delimiter) == 0 {
		return rune(0), ErrEmptyDelimiter
	}

	if len(delimiter) > 1 {
		return rune(0), ErrInvalidDelimiter
	}

	// Return the rune
	return rune(delimiter[0]), nil
}
