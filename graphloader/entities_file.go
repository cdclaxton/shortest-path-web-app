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

// An EntitiesCsvFile specifies the location and format of a single CSV file containing entities.
type EntitiesCsvFile struct {
	Path             string            `json:"path"`             // Location of the file
	EntityType       string            `json:"entityType"`       // Type of entities in the file
	Delimiter        string            `json:"delimiter"`        // Delimiter
	EntityIdField    string            `json:"entityIdField"`    // Name of the field with the entity ID
	FieldToAttribute map[string]string `json:"fieldToAttribute"` // Mapping of field name to attribute
}

// NewEntitiesCsvFile given the entity config.
func NewEntitiesCsvFile(path string, entityType string, delimiter string,
	entityIdField string, fieldToAttribute map[string]string) EntitiesCsvFile {

	return EntitiesCsvFile{
		Path:             path,
		EntityType:       entityType,
		Delimiter:        delimiter,
		EntityIdField:    entityIdField,
		FieldToAttribute: fieldToAttribute,
	}
}

// An EntitiesCsvFileReader reads and parses entities from a CSV file.
type EntitiesCsvFileReader struct {
	entitiesCsvFile     EntitiesCsvFile
	csvReader           *csv.Reader
	file                *os.File
	entityIdFieldIndex  int
	attributeFieldIndex map[string]int

	nextEntity       graphstore.Entity // Next entity
	hasNext          bool              // Is there another entity to read?
	numberOfEntities int               // Number of entities parsed
	numberOfRows     int               // Number of lines (>= number of entities + 1)
}

// NewEntitiesCsvFileReader given the CSV file config.
func NewEntitiesCsvFileReader(csv EntitiesCsvFile) *EntitiesCsvFileReader {
	return &EntitiesCsvFileReader{
		entitiesCsvFile: csv,
	}
}

// Initialise the CSV reader.
func (reader *EntitiesCsvFileReader) Initialise() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Opening CSV file of entities")

	// Open the file
	var err error
	reader.file, err = os.Open(reader.entitiesCsvFile.Path)
	if err != nil {
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Creating the CSV reader")

	// Parse delimiter
	delimiter, err := parseDelimiter(reader.entitiesCsvFile.Delimiter)
	if err != nil {
		reader.file.Close()
		return err
	}

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)
	reader.csvReader.Comma = delimiter

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Reading CSV file header")

	// Read the header from the file
	header, err := reader.csvReader.Read()
	if err != nil {
		reader.file.Close()
		return err
	}

	reader.numberOfRows += 1

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Finding index of the Entity ID field")

	// Find the entity ID field index
	fieldToIndex, err := findIndicesOfFields(header, []string{reader.entitiesCsvFile.EntityIdField})

	if err != nil {
		reader.file.Close()
		return err
	}

	reader.entityIdFieldIndex = fieldToIndex[reader.entitiesCsvFile.EntityIdField]

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Creating a mapping from an attribute to a field index")

	// Create the mapping from the attribute to the field index in the CSV file
	reader.attributeFieldIndex, err = attributeToFieldIndex(header, reader.entitiesCsvFile.FieldToAttribute)

	if err != nil {
		reader.file.Close()
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Reading the first Entity")

	// Read the first record
	reader.nextEntity, reader.hasNext = reader.readRecord()

	return nil
}

// readRecord from the CSV file containing entities.
func (reader *EntitiesCsvFileReader) readRecord() (graphstore.Entity, bool) {

	recordFound := false
	var record []string
	var entity graphstore.Entity

	for !recordFound {
		var err error
		record, err = reader.csvReader.Read()

		if err == io.EOF {
			// End of file
			return graphstore.Entity{}, false
		}

		reader.numberOfRows += 1

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Str("filepath", reader.entitiesCsvFile.Path).
				Err(err).
				Msg("Line failed to parse")
			continue
		}

		// Extract the entity ID
		entityId := record[reader.entityIdFieldIndex]

		// Extract the entity attributes
		attributes, err := extractAttributes(record, reader.attributeFieldIndex)

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Str("filepath", reader.entitiesCsvFile.Path).
				Err(err).
				Msg("Failed to extract attributes from record")
			continue
		}

		// Build the entity
		entity, err = graphstore.NewEntity(entityId, reader.entitiesCsvFile.EntityType, attributes)

		if err != nil {
			logging.Logger.Warn().
				Str(logging.ComponentField, componentName).
				Str("filepath", reader.entitiesCsvFile.Path).
				Err(err).
				Msg("Failed to build an entity from record")
			continue
		}

		// Increment the number of entities parsed from the CSV file
		reader.numberOfEntities += 1

		recordFound = true
	}

	return entity, true
}

// Next Entity from the file.
func (reader *EntitiesCsvFileReader) Next() (graphstore.Entity, error) {

	if !reader.hasNext {
		return graphstore.Entity{}, errors.New("Next() called when no next item exists")
	}

	// Get the current Entity
	current := reader.nextEntity

	// Try to read the next record
	reader.nextEntity, reader.hasNext = reader.readRecord()

	if reader.hasNext && reader.numberOfRows%100000 == 0 {
		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("numberOfRowsRead", strconv.Itoa(reader.numberOfRows)).
			Str("numberOfEntitiesRead", strconv.Itoa(reader.numberOfEntities)).
			Str("filepath", reader.entitiesCsvFile.Path).
			Msg("Reading documents from CSV file")
	}

	return current, nil
}

// Close the entities CSV file.
func (reader *EntitiesCsvFileReader) Close() error {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfRowsRead", strconv.Itoa(reader.numberOfRows)).
		Str("numberOfEntitiesRead", strconv.Itoa(reader.numberOfEntities)).
		Str("filepath", reader.entitiesCsvFile.Path).
		Msg("Closing CSV file")

	return reader.file.Close()
}

// ReadAll the entities from the CSV file.
func (reader *EntitiesCsvFileReader) ReadAll() ([]graphstore.Entity, error) {

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return nil, err
	}

	// Read all the entities from the file
	entities := []graphstore.Entity{}

	for reader.hasNext {
		entity, err := reader.Next()

		if err != nil {
			return entities, err
		}

		entities = append(entities, entity)
	}

	// Close the reader
	err = reader.Close()

	return entities, err
}
