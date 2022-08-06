package loader

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
)

type EntitiesCsvFile struct {
	Path             string            // Location of the file
	EntityType       string            // Type of entities in the file
	Delimiter        string            // Delimiter
	EntityIdField    string            // Name of the field with the entity ID
	FieldToAttribute map[string]string // Mapping of field name to attribute
}

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

type EntitiesCsvFileReader struct {
	entitiesCsvFile     EntitiesCsvFile
	csvReader           *csv.Reader
	file                *os.File
	entityIdFieldIndex  int
	attributeFieldIndex map[string]int

	nextEntity graphstore.Entity
	hasNext    bool
}

func NewEntitiesCsvFileReader(csv EntitiesCsvFile) *EntitiesCsvFileReader {
	return &EntitiesCsvFileReader{
		entitiesCsvFile: csv,
	}
}

func (reader *EntitiesCsvFileReader) Initialise() error {

	// Open the file
	var err error
	reader.file, err = os.Open(reader.entitiesCsvFile.Path)
	if err != nil {
		return err
	}

	// Create the CSV reader
	reader.csvReader = csv.NewReader(reader.file)

	// Read the header from the file
	header, err := reader.csvReader.Read()

	// Find the entity ID field index
	fieldToIndex, err := findIndicesOfFields(header, []string{reader.entitiesCsvFile.EntityIdField})

	if err != nil {
		return err
	}

	reader.entityIdFieldIndex = fieldToIndex[reader.entitiesCsvFile.EntityIdField]

	// Create the mapping from the attribute to the field index in the CSV file
	reader.attributeFieldIndex, err = attributeToFieldIndex(header, reader.entitiesCsvFile.FieldToAttribute)

	if err != nil {
		return err
	}

	// Read the first record
	reader.nextEntity, reader.hasNext = reader.readRecord()

	return nil
}

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

		if err != nil {
			// TODO log message
			continue
		}

		// Extract the entity ID
		entityId := record[reader.entityIdFieldIndex]

		// Extract the entity attributes
		attributes, err := extractAttributes(record, reader.attributeFieldIndex)

		if err != nil {
			// TODO log message
			continue
		}

		// Build the entity
		entity, err = graphstore.NewEntity(entityId, reader.entitiesCsvFile.EntityType, attributes)

		if err != nil {
			// TODO log message
			continue
		}

		recordFound = true
	}

	return entity, true
}

// Next Entity from the file.
func (reader *EntitiesCsvFileReader) Next() (graphstore.Entity, error) {

	if !reader.hasNext {
		return graphstore.Entity{}, fmt.Errorf("Next() called when no next item exists")
	}

	// Get the current Entity
	current := reader.nextEntity

	// Try to read the next record
	reader.nextEntity, reader.hasNext = reader.readRecord()

	return current, nil
}

// Close the entities CSV file.
func (reader *EntitiesCsvFileReader) Close() error {
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
