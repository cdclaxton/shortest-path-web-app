package i2chart

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/bfs"
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"golang.org/x/exp/maps"
)

// Keywords
const (
	entityIdKeyword       = "ID"
	entitySetNamesKeyword = "ENTITY-SET-NAMES"
)

type LinksSpec struct {
	Label         string `json:"label"` // Specification of the label connecting entities
	DateAttribute string `json:"dateAttribute"`
	DateFormat    string `json:"dateFormat"`
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
	config    I2ChartConfig                  // Configuration for the output
	bipartite graphstore.BipartiteGraphStore // Bipartite store
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

func (i *I2ChartBuilder) SetBipartite(bipartite graphstore.BipartiteGraphStore) {
	i.bipartite = bipartite
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
		return nil, fmt.Errorf("No documents in common for entities %v and %v", entity1.Id,
			entity2.Id)
	}

	// Documents in common given their IDs
	docs := []*graphstore.Document{}
	for _, docId := range docsInCommon.ToSlice() {
		doc := bipartite.GetDocument(docId)
		if doc == nil {
			return nil, fmt.Errorf("Unable to get document with ID %v", docId)
		}
		docs = append(docs, doc)
	}

	// Sort the documents by ID
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Id < docs[j].Id
	})

	return docs, nil
}

func substituteForLink(docs []*graphstore.Document, spec LinksSpec,
	missingAttribute string) (string, error) {

	// Keywords for the documents
	keywordToValue := keywordsForDocs(docs, spec.DateAttribute, spec.DateFormat)

	return Substitute(spec.Label, keywordToValue, missingAttribute)
}

func makeLinkLabel(entity1 *graphstore.Entity, entity2 *graphstore.Entity,
	bipartite graphstore.BipartiteGraphStore, spec LinksSpec,
	missingAttribute string) (string, error) {

	// Documents linking the two entities
	docs, err := documentsLinkingEntities(entity1, entity2, bipartite)
	if err != nil {
		return "", err
	}

	// Build the link label
	return substituteForLink(docs, spec, missingAttribute)
}

func mergeKeywords(m1 map[string]string, m2 map[string]string) map[string]string {
	merged := map[string]string{}

	for key, value := range m1 {
		merged[key] = value
	}

	for key, value := range m2 {
		merged[key] = value
	}

	return merged
}

func makeI2Entity(entity *graphstore.Entity, columns []string,
	entitySpec map[string]map[string]string, missingAttribute string,
	keywordToValue map[string]string) ([]string, error) {

	// Preconditions
	if entity == nil {
		return nil, fmt.Errorf("Nil entity")
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("No columns specified")
	}

	if len(entity.EntityType) == 0 {
		return nil, fmt.Errorf("Entity has an empty type")
	}

	// Get the specification of the fields given the entity type
	fieldSpecs, found := entitySpec[entity.EntityType]
	if !found {
		return nil, fmt.Errorf("Specification for entity type %v not found", entity.EntityType)
	}

	// Add the entity's attributes to the keywords and the entity's ID
	mergedKeywords := mergeKeywords(keywordToValue, entity.Attributes)
	mergedKeywords[entityIdKeyword] = entity.Id

	// Build the fields
	fields := make([]string, len(columns))
	for idx, column := range columns {

		specForColumn, found := fieldSpecs[column]
		if !found {
			return nil, fmt.Errorf("Field spec for %v not found", column)
		}

		field, err := Substitute(specForColumn, mergedKeywords, missingAttribute)
		if err != nil {
			return nil, err
		}

		fields[idx] = field
	}

	return fields, nil
}

// rowLinkingEntities given the specification for a row and the data.
func (i *I2ChartBuilder) rowLinkingEntities(entityId1 string, entityId2 string,
	keywordToValueEntity1 map[string]string,
	keywordToValueEntity2 map[string]string) ([]string, error) {

	// Preconditions
	if i.bipartite == nil {
		return nil, fmt.Errorf("Bipartite graph has not been defined")
	}

	// Get the entities from the store
	entity1 := i.bipartite.GetEntity(entityId1)
	if entity1 == nil {
		return nil, fmt.Errorf("Entity with ID %v not found in bipartite store", entityId1)
	}

	entity2 := i.bipartite.GetEntity(entityId2)
	if entity2 == nil {
		return nil, fmt.Errorf("Entity with ID %v not found in bipartite store", entityId2)
	}

	// Row
	row := make([]string, len(i.config.Columns)*2+1)

	// Add the fields for entity 1
	entity1Fields, err := makeI2Entity(entity1, i.config.Columns,
		i.config.Entities, i.config.AttributeNotKnown, keywordToValueEntity1)

	if err != nil {
		return nil, err
	}

	for idx := range entity1Fields {
		row[idx] = entity1Fields[idx]
	}

	// Add the fields for entity 2
	entity2Fields, err := makeI2Entity(entity2, i.config.Columns,
		i.config.Entities, i.config.AttributeNotKnown, keywordToValueEntity2)

	if err != nil {
		return nil, err
	}

	for idx := range entity1Fields {
		row[idx+len(i.config.Columns)] = entity2Fields[idx]
	}

	// Add the link
	linkLabel, err := makeLinkLabel(entity1, entity2, i.bipartite, i.config.Links,
		i.config.AttributeNotKnown)

	if err != nil {
		return nil, err
	}

	row[len(row)-1] = linkLabel

	// Return the constructed row
	return row, nil
}

// buildDatasetKeywords for a given entity.
func buildDatasetKeywords(entityId string, conns *bfs.NetworkConnections) (map[string]string, error) {

	// Preconditions
	if conns.EntityIdToSetNames == nil {
		return nil, fmt.Errorf("Mapping from entity ID to data set names is nil")
	}

	keywords := map[string]string{}

	if datasets, found := conns.EntityIdToSetNames[entityId]; found {
		sliceDatasets := datasets.ToSlice()
		sort.Strings(sliceDatasets)
		keywords[entitySetNamesKeyword] = strings.Join(sliceDatasets, ", ")
	} else {
		keywords[entitySetNamesKeyword] = ""
	}

	return keywords, nil
}

// Build the rows of the i2 chart from the network connections. The entity details are held
// within the bipartite graph store.
func (i *I2ChartBuilder) Build(conns *bfs.NetworkConnections) ([][]string, error) {

	// Preconditions
	if i.bipartite == nil {
		return nil, fmt.Errorf("Bipartite graph store is not defined")
	}

	if conns == nil {
		return nil, fmt.Errorf("Nil connections passed to Build")
	}

	// Unipartite graph to store the entities that are connected in the i2 chart
	i2Graph := graphstore.NewInMemoryUnipartiteGraphStore()

	rows := [][]string{}

	// Add the header row
	rows = append(rows, header(i.config.Columns))

	// Walk though each set of connected entities
	for _, destinationsPaths := range conns.Connections {
		for _, paths := range destinationsPaths {
			for _, path := range paths {

				// Check the path is valid
				if len(path.Route) == 0 {
					return nil, fmt.Errorf("Path with no entities encountered")
				} else if len(path.Route) == 1 {
					return nil, fmt.Errorf("Path has just one entity")
				}

				// Walk through each pair of entities on the path
				for idx := 0; idx < len(path.Route)-1; idx++ {
					src := path.Route[idx]
					dst := path.Route[idx+1]

					// An edge edge already exists between the two entities then a row doesn't
					// need to be added to the i2 chart
					if i2Graph.EdgeExists(src, dst) {
						continue
					}

					// Build the keywords
					keywordToValueEntity1, err := buildDatasetKeywords(src, conns)
					if err != nil {
						return nil, err
					}
					keywordToValueEntity2, err := buildDatasetKeywords(dst, conns)
					if err != nil {
						return nil, err
					}

					// Create the row
					row, err := i.rowLinkingEntities(src, dst, keywordToValueEntity1,
						keywordToValueEntity2)
					if err != nil {
						return nil, err
					}
					rows = append(rows, row)

					// Record that the row contains linked entities
					i2Graph.AddUndirected(src, dst)
				}
			}
		}
	}

	return rows, nil
}
