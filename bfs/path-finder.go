package bfs

import (
	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

type PathFinder struct {
	graph graphstore.UnipartiteGraphStore
}

// NetworkConnections stores the paths under a given length between entities.
type NetworkConnections struct {
	EntityIdToSetNames map[string]*set.Set[string]
	Connections        map[string]map[string][]Path
	MaxDepth           int
}

func NewNetworkConnections(maxDepth int) *NetworkConnections {
	return &NetworkConnections{
		EntityIdToSetNames: map[string]*set.Set[string]{},
		Connections:        map[string]map[string][]Path{},
		MaxDepth:           maxDepth,
	}
}

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

func (n *NetworkConnections) Equal(other *NetworkConnections) bool {
	return entityIdToSetsEqual(n.EntityIdToSetNames, other.EntityIdToSetNames) &&
		connectionsEqual(n.Connections, other.Connections) &&
		n.MaxDepth == other.MaxDepth
}

func (n *NetworkConnections) AddEntity(entity string, entitySet string) {

	// If the entity hasn't been seen before, then add an entry
	if _, found := n.EntityIdToSetNames[entity]; !found {
		s := set.NewSet[string]()
		n.EntityIdToSetNames[entity] = s
	}

	// Store the entity set that the entity appears in
	n.EntityIdToSetNames[entity].Add(entitySet)
}

func (n *NetworkConnections) AddConnections(entity1 string, entity1Set string,
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

// func (p *PathFinder) findAllPathsWithResilience(root string, goal string, maxDepth int) ([]Path, error) {

// 	paths, err := AllPaths(p.graph, root, goal, maxDepth)

// 	// If there are no errors, then just return
// 	if err == nil {
// 		return paths, nil
// 	}

// 	// Be resilient to missing root and goal vertices
// 	if strings.Contains(err.Error(), RootVertexNotFoundError) || strings.Contains(err.Error(), GoalVertexNotFoundError) {
// 		return paths, nil
// 	}

// 	return paths, err
// }

// func (p *PathFinder) pathsWithinEntitySet(entitySet job.EntitySet, maxDepth int) ([]EntityConnections, error) {

// 	connections := []EntityConnections{}

// 	for idx1, entityId1 := range entitySet.EntityIds {
// 		for idx2, entityId2 := range entitySet.EntityIds {

// 			// Ignore self-connections
// 			if idx1 == idx2 {
// 				continue
// 			}

// 			// Find all paths between entities
// 			paths, err := p.findAllPathsWithResilience(entityId1, entityId2, maxDepth)

// 			if err != nil {
// 				return nil, err
// 			}

// 			if len(paths) > 0 {

// 			}
// 		}
// 	}

// 	return connections, nil
// }

// func pathsBetweenEntitySets(entitySets []job.EntitySet) ([]Path, error) {

// }

// // FindPaths between the entities defined in the sets.
// func (p *PathFinder) FindPaths(entitySets []job.EntitySet, maxDepth int) ([]Path, error) {

// 	// Precondition
// 	if len(entitySets) == 0 {
// 		return nil, fmt.Errorf("No entity sets provided")
// 	}

// 	for _, entitySet := range entitySets {
// 		if len(entitySet.EntityIds) == 0 {
// 			return nil, fmt.Errorf("Blank entity set provided")
// 		}
// 	}

// 	if maxDepth < 0 {
// 		return nil, fmt.Errorf("Invalid maximum depth: %v", maxDepth)
// 	}

// 	// If there is only one entity set, then find the paths between those entities
// 	if len(entitySets) == 1 {
// 		return pathsWithinEntitySet(entitySets[0])
// 	}

// 	return pathsBetweenEntitySets(entitySets)
// }
