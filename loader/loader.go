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
