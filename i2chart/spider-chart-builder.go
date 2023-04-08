package i2chart

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
	"github.com/cdclaxton/shortest-path-web-app/spider"
)

var (
	ErrBipartiteIsNil     = errors.New("bipartite graph store is nil")
	ErrSpiderResultsIsNil = errors.New("spider results is nil")
	ErrEntityIsEmpty      = errors.New("entity ID is empty")
)

type SpiderEntityConfig struct {
	Icon  string `json:"icon"`  // Icon to use in i2
	Label string `json:"label"` // Label to use in i2
}

type SpiderI2ChartConfig struct {
	EntityConfig           map[string]SpiderEntityConfig `json:"entities"` // Key is the entity type
	UnknownEntityTypeIcon  string                        `json:"unknownEntityTypeIcon"`
	UnknownEntityTypeLabel string                        `json:"unknownEntityTypeLabel"`
	MissingAttribute       string                        `json:"missingAttribute"`
}

// readSpiderI2ChartConfig reads the i2 chart config for spidering from a JSON file.
func readSpiderI2ChartConfig(filepath string) (*SpiderI2ChartConfig, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", filepath).
		Msg("Reading spider i2 chart config from JSON file")

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
	config := SpiderI2ChartConfig{}
	err = json.Unmarshal(content, &config)

	if err != nil {
		return nil, err
	}

	return &config, nil
}

type SpiderChartBuilder struct {
	config    SpiderI2ChartConfig
	bipartite graphstore.BipartiteGraphStore // Bipartite store
}

func NewSpiderChartBuilder(filepath string) (*SpiderChartBuilder, error) {

	// Read the config from a JSON file
	config, err := readSpiderI2ChartConfig(filepath)
	if err != nil {
		return nil, err
	}

	return &SpiderChartBuilder{
		config: *config,
	}, nil
}

// SetBipartite graph store used by the i2 chart builder.
func (s *SpiderChartBuilder) SetBipartite(bipartite graphstore.BipartiteGraphStore) {
	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Setting bipartite graph store in the spider i2 chart builder")
	s.bipartite = bipartite
}

// sortedEntityIds returns a sorted list of entity IDs.
func sortedEntityIds(entityIds *set.Set[string]) []string {
	s := entityIds.ToSlice()
	sort.Strings(s)
	return s
}

type EntityForI2 struct {
	entityId     string
	entityType   string
	entityIcon   string
	entityLabel  string
	isSeedEntity string
}

type RowForI2 struct {
	entity1 EntityForI2
	entity2 EntityForI2
}

func (r *RowForI2) Serialise() []string {
	return []string{
		r.entity1.entityId,
		r.entity1.entityType,
		r.entity1.entityIcon,
		r.entity1.entityLabel,
		r.entity1.isSeedEntity,
		r.entity2.entityId,
		r.entity2.entityType,
		r.entity2.entityIcon,
		r.entity2.entityLabel,
		r.entity2.isSeedEntity,
	}
}

func makeEntityForI2(bipartite graphstore.BipartiteGraphStore, entityId string,
	entityIsSeed bool, config SpiderI2ChartConfig) (EntityForI2, error) {

	if bipartite == nil {
		return EntityForI2{}, ErrBipartiteIsNil
	}

	entity, err := bipartite.GetEntity(entityId)
	if err != nil {
		return EntityForI2{}, err
	}

	// Get the i2 config for the entity type
	entityTypeConfig, found := config.EntityConfig[entity.EntityType]

	// Get the entity icon
	var entityIcon string
	if !found {
		entityIcon = config.UnknownEntityTypeIcon
	} else {
		entityIcon = entityTypeConfig.Icon
	}

	// Make the entity label
	var entityLabel string
	if !found {
		entityLabel = config.UnknownEntityTypeLabel
	} else {
		fmt.Println(entity.Attributes)
		entityLabel, err = Substitute(entityTypeConfig.Label, entity.Attributes,
			config.MissingAttribute)
		if err != nil {
			return EntityForI2{}, nil
		}
	}

	// Make the entity seed
	var entitySeed string
	if entityIsSeed {
		entitySeed = "TRUE"
	} else {
		entitySeed = "FALSE"
	}

	return EntityForI2{
		entityId:     entityId,
		entityType:   entity.EntityType,
		entityIcon:   entityIcon,
		entityLabel:  entityLabel,
		isSeedEntity: entitySeed,
	}, nil
}

// makeSpiderRow constructs a row showing the connection between two entities.
func makeSpiderRow(bipartite graphstore.BipartiteGraphStore, entityId string,
	entityIsSeed bool, adjEntityId string, adjEntityIsSeed bool,
	config SpiderI2ChartConfig) (RowForI2, error) {

	if bipartite == nil {
		return RowForI2{}, ErrBipartiteIsNil
	}

	if len(entityId) == 0 {
		return RowForI2{}, ErrEntityIsEmpty
	}

	if len(adjEntityId) == 0 {
		return RowForI2{}, ErrEntityIsEmpty
	}

	firstEntityForI2, err := makeEntityForI2(bipartite, entityId, entityIsSeed, config)
	if err != nil {
		return RowForI2{}, err
	}

	secondEntityForI2, err := makeEntityForI2(bipartite, adjEntityId, adjEntityIsSeed, config)
	if err != nil {
		return RowForI2{}, err
	}

	return RowForI2{
		entity1: firstEntityForI2,
		entity2: secondEntityForI2,
	}, nil
}

// Build the rows of the i2 chart.
// The structure is:
//   entity ID, type, icon, label, seed, entity ID, type, icon, label, seed
func (s *SpiderChartBuilder) Build(results *spider.SpiderResults) ([][]string, error) {

	if s.bipartite == nil {
		return nil, ErrBipartiteIsNil
	}

	if results == nil {
		return nil, ErrSpiderResultsIsNil
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfSteps", strconv.Itoa(results.NumberSteps)).
		Str("numberOfSeedEntities", strconv.Itoa(results.SeedEntities.Len())).
		Str("numberOfSeedEntitiesNotFound", strconv.Itoa(results.SeedEntitiesNotFound.Len())).
		Msg("Building spider i2 chart")

	// Rows to write to the Excel file
	rows := [][]string{}

	// Add the header row
	headerRow := RowForI2{
		entity1: EntityForI2{
			entityId:     "ID-1",
			entityType:   "Type-1",
			entityIcon:   "Icon-1",
			entityLabel:  "Label-1",
			isSeedEntity: "Seed-1",
		},
		entity2: EntityForI2{
			entityId:     "ID-2",
			entityType:   "Type-2",
			entityIcon:   "Icon-2",
			entityLabel:  "Label-2",
			isSeedEntity: "Seed-2",
		},
	}
	rows = append(rows, headerRow.Serialise())

	// Get a sorted list of entity IDs to ensure the rows are always in the same order
	unsortedEntityIds, err := results.Subgraph.EntityIds()
	if err != nil {
		return nil, err
	}

	// Walk through each entity ID and add its connections to the rows
	for _, entityId := range sortedEntityIds(unsortedEntityIds) {

		// Get a set of the adjacent entities
		adjEntityIds, err := results.Subgraph.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return nil, err
		}

		// Walk through the sorted the adjacent entity IDs (to ensure a consistent output)
		for _, adjEntityId := range sortedEntityIds(adjEntityIds) {

			if entityId > adjEntityId {
				continue
			}

			entityIsSeed := results.SeedEntities.Has(entityId)
			adjEntityIsSeed := results.SeedEntities.Has(adjEntityId)

			row, err := makeSpiderRow(s.bipartite,
				entityId, entityIsSeed,
				adjEntityId, adjEntityIsSeed,
				s.config)

			if err != nil {
				return nil, err
			}

			rows = append(rows, row.Serialise())
		}
	}

	return rows, nil
}
