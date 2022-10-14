package bfs

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/job"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

type PathFinder struct {
	graph graphstore.UnipartiteGraphStore
}

func NewPathFinder(graph graphstore.UnipartiteGraphStore) *PathFinder {
	return &PathFinder{
		graph: graph,
	}
}

// NetworkConnections stores the paths under a given length between entities of interest.
type NetworkConnections struct {
	EntityIdToSetNames map[string]*set.Set[string]
	Connections        map[string]map[string][]Path
	MaxHops            int
}

func (n *NetworkConnections) hasDirectedConnection(entity1 string, entity2 string) bool {
	destinations, found := n.Connections[entity1]
	if !found {
		return false
	}

	_, found = destinations[entity2]
	return found
}

func (n *NetworkConnections) HasConnection(entity1 string, entity2 string) bool {
	return n.hasDirectedConnection(entity1, entity2) || n.hasDirectedConnection(entity2, entity1)
}

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

// NewNetworkConnections struct given a maximum number of hops from source to destination.
func NewNetworkConnections(maxHops int) *NetworkConnections {
	return &NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{},
		Connections:        map[string]map[string][]Path{},
		MaxHops:            maxHops,
	}
}

// entityIdToSetsEqual returns true if the entity ID to set of names are equal.
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
func (n *NetworkConnections) Equal(other *NetworkConnections) bool {
	return entityIdToSetsEqual(n.EntityIdToSetNames, other.EntityIdToSetNames) &&
		connectionsEqual(n.Connections, other.Connections) &&
		n.MaxHops == other.MaxHops
}

// AddEntity and the data set to which it belongs.
func (n *NetworkConnections) AddEntity(entity string, entitySet string) {

	// If the entity hasn't been seen before, then add an entry
	if _, found := n.EntityIdToSetNames[entity]; !found {
		s := set.NewSet[string]()
		n.EntityIdToSetNames[entity] = s
	}

	// Store the entity set that the entity appears in
	n.EntityIdToSetNames[entity].Add(entitySet)
}

// AddPaths between two entities.
func (n *NetworkConnections) AddPaths(entity1 string, entity1Set string,
	entity2 string, entity2Set string, paths []Path) {

	// Insert the entities
	n.AddEntity(entity1, entity1Set)
	n.AddEntity(entity2, entity2Set)

	// Add the connections
	if _, found := n.Connections[entity1]; !found {
		n.Connections[entity1] = map[string][]Path{}
	}
	n.Connections[entity1][entity2] = paths
}

// findAllPathsWithResilience to (potentially missing) root and goal vertices.
func (p *PathFinder) findAllPathsWithResilience(root string, goal string,
	maxHops int) ([]Path, error) {

	// Find all paths between the root and the goal entities
	paths, err := AllPaths(p.graph, root, goal, maxHops)

	// If there are no errors, then just return
	if err == nil {
		return paths, nil
	}

	// Be resilient to missing root and goal vertices
	if strings.Contains(err.Error(), RootVertexNotFoundError) || strings.Contains(err.Error(),
		GoalVertexNotFoundError) {
		return paths, nil
	}

	return paths, err
}

// pathsBetweenEntitySets returns all paths between two sets of entities given a maximum number of
// hops. The connection between an entity and itself is ignored.
func (p *PathFinder) pathsBetweenEntitySets(entitySet1 job.EntitySet, entitySet2 job.EntitySet,
	connections *NetworkConnections) error {

	// Walk through all pairs of entities
	for _, entityId1 := range entitySet1.EntityIds {

		connections.AddEntity(entityId1, entitySet1.Name)

		for _, entityId2 := range entitySet2.EntityIds {

			connections.AddEntity(entityId2, entitySet2.Name)

			// Ignore self-connections
			if entityId1 == entityId2 {
				continue
			}

			// Skip finding paths that have already been found
			if connections.HasConnection(entityId1, entityId2) {
				continue
			}

			// Find all paths between entities
			paths, err := p.findAllPathsWithResilience(entityId1, entityId2, connections.MaxHops)

			if err != nil {
				return err
			}

			if len(paths) > 0 {
				connections.AddPaths(entityId1, entitySet1.Name, entityId2, entitySet2.Name, paths)
			}
		}
	}

	return nil
}

// pathsBetweenAllEntitySets finds the paths (within a given number of hops) between entities
// in the provided sets.
func (p *PathFinder) pathsBetweenAllEntitySets(entitySets []job.EntitySet,
	connections *NetworkConnections) error {

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

	// Precondition
	if len(entitySets) == 0 {
		return nil, fmt.Errorf("No entity sets provided")
	}

	for _, entitySet := range entitySets {
		if len(entitySet.EntityIds) == 0 {
			return nil, fmt.Errorf("Blank entity set provided")
		}
	}

	if maxHops < 0 {
		return nil, fmt.Errorf("Invalid maximum number of hops: %v", maxHops)
	}

	// New struct to hold the network connections
	connections := NewNetworkConnections(maxHops)

	// If there is only one entity set, then find the paths between those entities, otherwise
	// find the paths between pairs of entity sets
	var err error
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
