package i2chart

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"golang.org/x/exp/maps"
)

type LinksSpec struct {
	Label string `json:"label"` // Specification of the label connecting entities
}

// An entity is the specification of the fields for a given entity type. By making this field
// highly configurable, it will be easy to add or remove fields in a deployed system.
type I2ChartConfig struct {
	Columns           []string                     `json:"columns"`           // Ordered list of columns for each entity
	Entities          map[string]map[string]string `json:"entities"`          // Specification for each entity type
	Links             LinksSpec                    `json:"links"`             // Link specification
	AttributeNotKnown string                       `json:"attributeNotKnown"` // Label to use for an unknown attribute
}

// readI2Config in a JSON file.
func readI2Config(filepath string) (*I2ChartConfig, error) {

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	// Ensure the file is closed
	defer file.Close()

	// Read the JSON into a byte array
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Unmarshall the data
	config := I2ChartConfig{}
	err = json.Unmarshal(content, &config)

	if err != nil {
		return nil, err
	}

	return &config, nil
}

// validateI2Config and returns whether it passes and a list of issues.
func validateI2Config(config I2ChartConfig) (bool, []string) {

	// Are entities defined?
	if len(config.Entities) == 0 {
		return false, []string{"No entities are defined"}
	}

	// Is the ordering of the entity columns defined?
	if len(config.Columns) == 0 {
		return false, []string{"Ordering of the entity columns is not defined"}
	}

	// Make a set of the entity columns
	expectedEntityColumns := set.NewPopulatedSet(config.Columns...)

	// Are the columns for each entity type consistent?
	entityIssues := []string{}
	for entityType, entitySpec := range config.Entities {

		// Set of column names for the entity
		columnNames := maps.Keys(entitySpec)
		setColumnNames := set.NewPopulatedSet(columnNames...)

		// Are there any columns missing given the expected columns?
		missingColumns := expectedEntityColumns.Difference(setColumnNames)
		for _, m := range missingColumns.ToSlice() {
			msg := fmt.Sprintf("Entity type %v is missing column %v", entityType, m)
			entityIssues = append(entityIssues, msg)
		}

		// Are there any extra columns?
		extraColumns := setColumnNames.Difference(expectedEntityColumns)
		for _, m := range extraColumns.ToSlice() {
			msg := fmt.Sprintf("Entity type %v has extra column %v", entityType, m)
			entityIssues = append(entityIssues, msg)
		}
	}

	if len(entityIssues) != 0 {
		return false, entityIssues
	}

	// Are the links defined?
	if len(config.Links.Label) == 0 {
		return false, []string{"Empty specification for a link label"}
	}

	// Is there an attribute not known label?
	if len(config.AttributeNotKnown) == 0 {
		return false, []string{"Attribute not known field is blank"}
	}

	return true, nil
}

type I2ChartBuilder struct {
	config I2ChartConfig
}

func NewI2ChartBuilder(filepath string) (*I2ChartBuilder, error) {

	// Read the config from a JSON file
	config, err := readI2Config(filepath)
	if err != nil {
		return nil, err
	}

	// Perform limited validation of the config (full validation would require knowing the
	// attributes of each entity type)
	isValid, reasons := validateI2Config(*config)
	if !isValid {
		return nil, fmt.Errorf("I2 chart builder config is invalid: %v",
			strings.Join(reasons, "; "))
	}

	return &I2ChartBuilder{
		config: *config,
	}, nil
}

// header of the i2 chart.
func header(entityColumns []string) []string {

	row := []string{}

	// First entity
	for _, column := range entityColumns {
		row = append(row, "Entity-"+column+"-1")
	}

	// Second entity
	for _, column := range entityColumns {
		row = append(row, "Entity-"+column+"-2")
	}

	// Link
	row = append(row, "Link")

	return row
}

// documentsLinkingEntities are those documents that are shared by the two entities.
func documentsLinkingEntities(entity1 *graphstore.Entity, entity2 *graphstore.Entity,
	bipartite graphstore.BipartiteGraphStore) ([]*graphstore.Document, error) {

	// Sets of document IDs
	docs1 := entity1.LinkedDocumentIds
	docs2 := entity2.LinkedDocumentIds

	// Document IDs in common between the two entities
	docsInCommon := docs1.Intersection(docs2)
	if docsInCommon.Len() == 0 {
		return nil, fmt.Errorf("No documents in common for entities %v and %v", entity1.Id, entity2.Id)
	}

	// Documents in common given their IDs
	docs := []*graphstore.Document{}
	for _, docId := range docsInCommon.ToSlice() {
		doc := bipartite.GetDocument(docId)
		if doc == nil {
			return nil, fmt.Errorf("Unable to get document with ID %v", docId)
		}
	}

	return docs, nil
}

func link(docs []*graphstore.Document, dateAttribute string, dateFormat string,
	linkFormat string, missingAttribute string) (string, error) {

	// Keywords for the documents
	keywordToValue := keywordsForDocs(docs, dateAttribute, dateFormat)

	return Substitute(linkFormat, keywordToValue, missingAttribute)
}

//
func (i *I2ChartBuilder) rowLinkingEntities(entityId1 string, entityId2 string,
	bipartite graphstore.BipartiteGraphStore) ([]string, error) {

	// Get the entities from the store
	entity1 := bipartite.GetEntity(entityId1)
	if entity1 == nil {
		return nil, fmt.Errorf("Entity with ID %v not found in bipartite store", entityId1)
	}

	entity2 := bipartite.GetEntity(entityId2)
	if entity2 == nil {
		return nil, fmt.Errorf("Entity with ID %v not found in bipartite store", entityId2)
	}

	return []string{}, nil
}

// Build the rows of the i2 chart from the network connections. The entity details are held
// within the bipartite graph store.
func (i *I2ChartBuilder) Build(conns *bfs.NetworkConnections,
	bipartite graphstore.BipartiteGraphStore) [][]string {

	rows := [][]string{}

	// Add the header row
	rows = append(rows, header(i.config.Columns))

	return rows
}
