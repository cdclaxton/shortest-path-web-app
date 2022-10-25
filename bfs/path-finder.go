package bfs

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// Component name to use in logging
const componentName = "bfs"

// Errors
var (
	ErrInvalidHops             = fmt.Errorf("Invalid number of hops")
	ErrUnipartiteGraphIsNil    = fmt.Errorf("Unipartite graph is nil")
	ErrEmptyEntityId           = fmt.Errorf("Entity ID is empty")
	ErrEmptyEntityDataset      = fmt.Errorf("Entity dataset is empty")
	ErrPathsIsNil              = fmt.Errorf("Paths is nil")
	ErrPathsIsEmpty            = fmt.Errorf("Slice of paths is empty")
	ErrNetworkConnectionsIsNil = fmt.Errorf("Network connections is nil")
	ErrEntitySetsIsNil         = fmt.Errorf("Entity sets is nil")
	ErrEntitySetsIsEmpty       = fmt.Errorf("Entity sets is empty")
	ErrNoEntitiesInEntitySet   = fmt.Errorf("No entity IDS in entity set")
	ErrNoNameForEntitySet      = fmt.Errorf("No name for entity set")
)

// PathFinder uses an unidirected unipartite graph to find paths from one entity to another.
type PathFinder struct {
	graph graphstore.UnipartiteGraphStore
}

// NewPathFinder given a unipartite graph.
func NewPathFinder(graph graphstore.UnipartiteGraphStore) (*PathFinder, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Creating a new path finder")

	// Precondition
	if graph == nil {
		return nil, ErrUnipartiteGraphIsNil
	}

	return &PathFinder{
		graph: graph,
	}, nil
}

// NetworkConnections stores the paths under a given length between entities of interest and it
// is populated by PathFinder.
//
// Entities at the source and destination of a path are of interest to the end-user and they are
// derived from datasets. An entity could appear in multiple datasets. The field EntityIdToSetNames
// stores the datasets in which the (source or destination) entity occurred.
//
// The Connections field stores the paths from one entity to another. There could be multiple
// paths between the entities, hence the paths are stored as a slice.
//
// The MaxHops field holds the maximum number of hops from a source entity to a destination
// entity. It should be a positive integer greater than zero.
type NetworkConnections struct {
	EntityIdToSetNames map[string]*set.Set[string]  // Entity ID to dataset name mapping
	Connections        map[string]map[string][]Path // Source to destination to list of paths connecting them
	MaxHops            int                          // Maximum number of hops from source to destination
}

// NewNetworkConnections struct given a maximum number of hops from source to destination.
func NewNetworkConnections(maxHops int) (*NetworkConnections, error) {

	// Precondition
	if maxHops < 1 {
		return nil, ErrInvalidHops
	}

	return &NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{},
		Connections:        map[string]map[string][]Path{},
		MaxHops:            maxHops,
	}, nil
}

// hasDirectedConnection returns true if there is a directed connection from entity1 to entity2.
func (n *NetworkConnections) hasDirectedConnection(entity1 string, entity2 string) bool {

	// Is entity1 present as a source?
	destinations, found := n.Connections[entity1]
	if !found {
		return false
	}

	// Is entity2 present as a destination?
	_, found = destinations[entity2]
	return found
}

// HasConnection returns true if entity1 and entity2 are connected by a (calculated) path.
func (n *NetworkConnections) HasConnection(entity1 string, entity2 string) (bool, error) {

	// Preconditions
	if entity1 == "" || entity2 == "" {
		return false, ErrEmptyEntityId
	}

	return n.hasDirectedConnection(entity1, entity2) || n.hasDirectedConnection(entity2, entity1), nil
}

// String representation of the network connections for debug and testing purposes.
func (n *NetworkConnections) String() string {
	var sb strings.Builder

	sb.WriteString("Network connections (max hops = " + strconv.Itoa(n.MaxHops) + "):\n")
	sb.WriteString("  Entity ID to set names:\n")
	for name, sets := range n.EntityIdToSetNames {
		sb.WriteString("    " + name + ": ")
		sb.WriteString(sets.String() + "\n")
	}

	sb.WriteString("  Connections:\n")
	for start := range n.Connections {
		for end, paths := range n.Connections[start] {
			sb.WriteString("    " + start + "->" + end + ":")
			for pathIdx, p := range paths {

				if pathIdx > 0 {
					sb.WriteString(", ")
				}

				sb.WriteString(" {")

				for idx, node := range p.Route {
					if idx > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString(node)
				}
				sb.WriteString("}")
			}
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// entityIdToSetsEqual returns true if the entity ID to set of dataset names are equal.
func entityIdToSetsEqual(e1 map[string]*set.Set[string], e2 map[string]*set.Set[string]) bool {

	// Check the entity IDs
	keys1 := set.NewSet[string]()
	for name := range e1 {
		keys1.Add(name)
	}

	keys2 := set.NewSet[string]()
	for name := range e2 {
		keys2.Add(name)
	}

	if !keys1.Equal(keys2) {
		return false
	}

	for name := range e1 {
		if !e1[name].Equal(e2[name]) {
			return false
		}
	}

	return true
}

// connectionsEqual returns true if the same entities are linked by the same paths.
func connectionsEqual(c1 map[string]map[string][]Path, c2 map[string]map[string][]Path) bool {

	// Use c1 as the reference
	for name1 := range c1 {
		for name2 := range c1[name1] {

			if _, found := c2[name1][name2]; !found {
				return false
			}

			if !PathsEqual(c1[name1][name2], c2[name1][name2]) {
				return false
			}
		}
	}

	// Use c2 as the reference
	for name1 := range c2 {
		for name2 := range c2[name1] {

			if _, found := c1[name1][name2]; !found {
				return false
			}

			if !PathsEqual(c1[name1][name2], c2[name1][name2]) {
				return false
			}
		}
	}

	return true
}

// Equal network connections?
//
// This function returns true if the network connections are identical, i.e. each source entity
// is connected to the same destination entities by the same paths.
func (n *NetworkConnections) Equal(other *NetworkConnections) bool {
	return entityIdToSetsEqual(n.EntityIdToSetNames, other.EntityIdToSetNames) &&
		connectionsEqual(n.Connections, other.Connections) &&
		n.MaxHops == other.MaxHops
}

// AddEntity and the dataset to which it belongs.
func (n *NetworkConnections) AddEntity(entity string, entitySet string) error {

	// Preconditions
	if len(entity) == 0 {
		return ErrEmptyEntityId
	}

	if len(entitySet) == 0 {
		return ErrEmptyEntityDataset
	}

	// If the entity hasn't been seen before, then add an entry
	if _, found := n.EntityIdToSetNames[entity]; !found {
		n.EntityIdToSetNames[entity] = set.NewSet[string]()
	}

	// Store the entity set that the entity appears in
	n.EntityIdToSetNames[entity].Add(entitySet)

	return nil
}

// AddPaths between two entities.
func (n *NetworkConnections) AddPaths(entity1 string, entity1Set string,
	entity2 string, entity2Set string, paths []Path) error {

	// Preconditions
	if paths == nil {
		return ErrPathsIsNil
	}

	if len(paths) == 0 {
		return ErrPathsIsEmpty
	}

	// Insert the entities
	err := n.AddEntity(entity1, entity1Set)
	if err != nil {
		return err
	}

	err = n.AddEntity(entity2, entity2Set)
	if err != nil {
		return err
	}

	// If entity1 hasn't been seen before, then add it as a source entity
	if _, found := n.Connections[entity1]; !found {
		n.Connections[entity1] = map[string][]Path{}
	}

	// Add the connections
	n.Connections[entity1][entity2] = paths

	return nil
}

// findAllPathsWithResilience to (potentially missing) root and goal vertices.
func (p *PathFinder) findAllPathsWithResilience(root string, goal string,
	maxHops int) ([]Path, error) {

	// Preconditions
	if len(root) == 0 {
		return nil, ErrEmptyEntityId
	}

	if len(goal) == 0 {
		return nil, ErrEmptyEntityId
	}

	if maxHops < 1 {
		return nil, ErrInvalidHops
	}

	// Find all paths between the root and the goal entities
	paths, err := AllPaths(p.graph, root, goal, maxHops)

	// If there are no errors, then just return
	if err == nil {
		return paths, nil
	}

	// Be resilient to missing root and goal vertices
	if strings.Contains(err.Error(), RootVertexNotFoundError) ||
		strings.Contains(err.Error(), GoalVertexNotFoundError) {
		return paths, nil
	}

	return paths, err
}

// pathsBetweenEntitySets returns all paths between two sets of entities given a maximum number of
// hops. The connection between an entity and itself is ignored.
func (p *PathFinder) pathsBetweenEntitySets(entitySet1 job.EntitySet, entitySet2 job.EntitySet,
	connections *NetworkConnections) error {

	// Preconditions
	if connections == nil {
		return ErrNetworkConnectionsIsNil
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("entitySet1", entitySet1.Name).
		Str("entitySet2", entitySet2.Name).
		Msg("Finding paths between entity sets")

	// Walk through all pairs of entities
	for _, entityId1 := range entitySet1.EntityIds {

		if err := connections.AddEntity(entityId1, entitySet1.Name); err != nil {
			return err
		}

		for _, entityId2 := range entitySet2.EntityIds {

			if err := connections.AddEntity(entityId2, entitySet2.Name); err != nil {
				return err
			}

			// Ignore self-connections
			if entityId1 == entityId2 {
				continue
			}

			// Skip finding paths that have already been found
			found, err := connections.HasConnection(entityId1, entityId2)

			if err != nil {
				return err
			}

			if found {
				// A path from entityId1 --> entityId2 already exists so there's no need to try
				// to find the paths again
				continue
			}

			// Find all paths between entities
			paths, err := p.findAllPathsWithResilience(entityId1, entityId2, connections.MaxHops)

			if err != nil {
				return err
			}

			if len(paths) > 0 {
				err := connections.AddPaths(entityId1, entitySet1.Name, entityId2, entitySet2.Name, paths)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// pathsBetweenAllEntitySets finds the paths (within a given number of hops) between entities
// in the provided sets.
func (p *PathFinder) pathsBetweenAllEntitySets(entitySets []job.EntitySet,
	connections *NetworkConnections) error {

	// Preconditions
	if entitySets == nil {
		return ErrEntitySetsIsNil
	}

	if len(entitySets) == 0 {
		return ErrEntitySetsIsEmpty
	}

	if connections == nil {
		return ErrNetworkConnectionsIsNil
	}

	// Walk through all distinct pairs of entity sets
	for entitySet1Index := range entitySets {
		for entitySet2Index := range entitySets {

			if entitySet2Index <= entitySet1Index {
				continue
			}

			// Find the paths between the two entity sets
			err := p.pathsBetweenEntitySets(entitySets[entitySet1Index],
				entitySets[entitySet2Index], connections)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// FindPaths between the entities defined in the sets.
func (p *PathFinder) FindPaths(entitySets []job.EntitySet, maxHops int) (
	*NetworkConnections, error) {

	// Preconditions
	if entitySets == nil {
		return nil, ErrEntitySetsIsNil
	}

	if len(entitySets) == 0 {
		return nil, ErrEntitySetsIsEmpty
	}

	for _, entitySet := range entitySets {
		if len(entitySet.Name) == 0 {
			return nil, ErrNoNameForEntitySet
		}

		if len(entitySet.EntityIds) == 0 {
			return nil, ErrNoEntitiesInEntitySet
		}
	}

	if maxHops < 0 {
		return nil, ErrInvalidHops
	}

	// Log the datasets
	datasets := []string{}
	for _, entitySet := range entitySets {
		datasets = append(datasets, entitySet.Name)
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfHops", strconv.Itoa(maxHops)).
		Str("numberOfDatasets", strconv.Itoa(len(entitySets))).
		Strs("datasets", datasets).
		Msg("Finding paths")

	// New struct to hold the network connections between entities
	connections, err := NewNetworkConnections(maxHops)
	if err != nil {
		return nil, err
	}

	// If there is only one entity set, then find the paths between those entities, otherwise
	// find the paths between pairs of entity sets
	if len(entitySets) == 1 {
		err = p.pathsBetweenEntitySets(entitySets[0], entitySets[0], connections)
	} else {
		err = p.pathsBetweenAllEntitySets(entitySets, connections)
	}

	if err != nil {
		return nil, err
	}

	return connections, nil
}
