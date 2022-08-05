package loader

import (
	"fmt"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
)

type GraphStoreLoaderFromCsv struct {
	graphStore    *graphstore.GraphStore
	entityFiles   []EntitiesCsvFile
	documentFiles []DocumentsCsvFile
	linkFiles     []LinksCsvFile
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
func attributeToFieldIndex(header []string, fieldToAttribute map[string]string) (map[string]int, error) {

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
func extractAttributes(row []string, attributeToFieldIndex map[string]int) (map[string]string, error) {

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
