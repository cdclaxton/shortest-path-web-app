package search

import (
	"errors"
	"sort"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

const componentName = "search"

// EntitySearch finds entities in the bipartite and unipartite stores.
type EntitySearch struct {
	Bipartite  graphstore.BipartiteGraphStore
	Unipartite graphstore.UnipartiteGraphStore
}

// NewEntitySearch given the bipartite and unipartite stores.
func NewEntitySearch(bipartite graphstore.BipartiteGraphStore,
	unipartite graphstore.UnipartiteGraphStore) (*EntitySearch, error) {

	// Preconditions
	if bipartite == nil {
		return nil, errors.New("bipartite graph is nil")
	}

	if unipartite == nil {
		return nil, errors.New("unipartite graph is nil")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Making the entity search engine")

	return &EntitySearch{
		Bipartite:  bipartite,
		Unipartite: unipartite,
	}, nil
}

// EntitySearchResult for a single entity.
type EntitySearchResult struct {
	InUnipartite bool
	InBipartite  bool
}

// Search for entities given their IDs in the bipartite and unipartite stores.
func (es *EntitySearch) Search(entityIds []string) (map[string]EntitySearchResult, error) {

	searchResult := map[string]EntitySearchResult{}

	for _, entityId := range entityIds {

		// Try to find the entity in the bipartite graph
		var entityInBipartite bool
		_, err := es.Bipartite.GetEntity(entityId)
		if err == graphstore.ErrEntityNotFound {
			entityInBipartite = false
		} else if err != nil {
			return nil, err
		} else {
			entityInBipartite = true
		}

		// Try to find the entity in the unipartite store
		entityInUnipartite, err := es.Unipartite.HasEntity(entityId)
		if err != nil {
			return nil, err
		}

		searchResult[entityId] = EntitySearchResult{
			InUnipartite: entityInUnipartite,
			InBipartite:  entityInBipartite,
		}
	}

	return searchResult, nil
}

// Attribute is a key-value pair for an entity or a document.
type Attribute struct {
	Key   string
	Value string
}

// ErrorDetails holds details about the presence or absence of an error.
type ErrorDetails struct {
	ErrorOccurred bool
	ErrorMessage  string
}

// BipartiteDocument holds details of a document from the bipartite store.
type BipartiteDocument struct {
	DocumentId   string      // Unique ID
	FoundInStore bool        // Found in the bipartite graph store?
	Type         string      // Document type
	Attributes   []Attribute // Sorted list of attributes
}

// BipartiteDetails for an entity derived from the bipartite store.
type BipartiteDetails struct {
	InBipartite      bool                // Is the entity in the bipartite store?
	EntityType       string              // Entity type, e.g. Person
	EntityAttributes []Attribute         // Sorted list of entity attributes
	LinkedDocuments  []BipartiteDocument // Sorted list of documents linked to the entity
}

// EntityPresence holds whether the entity exists in the bipartite and unipartite stores.
type EntityPresence struct {
	EntityId     string // Unique entity ID
	InBipartite  bool   // Is the entity in the bipartite store?
	InUnipartite bool   // Is the entity in the unipartite store?
}

// SearchEntity is the result of search for an entity in the bipartite and unipartite stores.
type SearchEntity struct {
	EntityId         string           // Unique entity ID
	Error            ErrorDetails     // Error that occurred whilst finding the entity
	BipartiteDetails BipartiteDetails // Entity information from the bipartite store
	InUnipartite     bool             // Is the entity in the unipartite store?
	LinkedEntities   []EntityPresence // Entities linked to the entity of interest
}

// NewSearchEntity instantiates a SearchEntity struct for a given entity ID.
func NewSearchEntity(entityId string) SearchEntity {
	return SearchEntity{
		EntityId:         entityId,
		Error:            ErrorDetails{},
		BipartiteDetails: BipartiteDetails{},
	}
}

// convertAndSortAttributes extracts and sorts the attributes.
func convertAndSortAttributes(attributes map[string]string) []Attribute {
	result := []Attribute{}

	// Extract the key-value pairs
	for key, value := range attributes {
		result = append(result, Attribute{
			Key:   key,
			Value: value,
		})
	}

	// Perform the sort on the key
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// extractDocuments from the bipartite store given their document IDs.
func (es *EntitySearch) extractDocuments(docIds *set.Set[string]) ([]BipartiteDocument, error) {

	docs := []BipartiteDocument{}

	// Get each of the documents
	for _, docId := range docIds.ToSlice() {

		// Try to get the document from the bipartite store
		doc, err := es.Bipartite.GetDocument(docId)
		if err == graphstore.ErrDocumentNotFound {

			// Document could not be found
			docs = append(docs, BipartiteDocument{
				DocumentId:   docId,
				FoundInStore: false,
				Type:         "",
				Attributes:   []Attribute{},
			})
			continue

		} else if err != nil {

			// An error occurred trying to get the document
			return []BipartiteDocument{}, err
		}

		// The document was retrieved successfully
		docs = append(docs, BipartiteDocument{
			DocumentId:   doc.Id,
			FoundInStore: true,
			Type:         doc.DocumentType,
			Attributes:   convertAndSortAttributes(doc.Attributes),
		})
	}

	// Sort the documents
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].DocumentId < docs[j].DocumentId
	})

	return docs, nil
}

// entityToBipartiteDetails gets the entity information from the bipartite store.
func (es *EntitySearch) entityToBipartiteDetails(bipartiteEntity *graphstore.Entity) (BipartiteDetails, error) {

	// Preconditions
	if bipartiteEntity == nil {
		return BipartiteDetails{}, errors.New("bipartite entity is nil")
	}

	found, err := es.Bipartite.HasEntityWithId(bipartiteEntity.Id)
	if !found || err != nil {
		return BipartiteDetails{}, errors.New("bipartite entity not found")
	}

	// Extract the documents associated with the entity from the bipartite store
	documents, err := es.extractDocuments(bipartiteEntity.LinkedDocumentIds)
	if err != nil {
		return BipartiteDetails{}, err
	}

	return BipartiteDetails{
		InBipartite:      true,
		EntityType:       bipartiteEntity.EntityType,
		EntityAttributes: convertAndSortAttributes(bipartiteEntity.Attributes),
		LinkedDocuments:  documents,
	}, nil
}

func (es *EntitySearch) entityIdsFromBipartite(entityId string) *set.Set[string] {

	// If the entity cannot be found in the bipartite store, then just return an empty
	// set of entity IDs
	entity, err := es.Bipartite.GetEntity(entityId)
	if err == graphstore.ErrEntityNotFound {
		return set.NewSet[string]()
	}

	// Set of all entity IDs
	linkedEntityIds := set.NewSet[string]()

	for _, docId := range entity.LinkedDocumentIds.ToSlice() {

		// Try to get the document from the bipartite store, being robust to errors
		document, err := es.Bipartite.GetDocument(docId)
		if err != nil {
			continue
		}

		for _, linkedEntityId := range document.LinkedEntityIds.ToSlice() {
			if linkedEntityId != entityId {
				linkedEntityIds.Add(linkedEntityId)
			}
		}
	}

	return linkedEntityIds
}

// linkedEntityPresence returns the entity existence for entities linked to a central entity.
func (es *EntitySearch) linkedEntityPresence(entityId string) ([]EntityPresence, error) {

	// Is the entity in the unipartite graph store?
	inUnipartite, err := es.Unipartite.HasEntity(entityId)
	if err != nil {
		return []EntityPresence{}, err
	}

	// Get the adjacent entity IDs from the unipartite store
	var entityIds *set.Set[string]
	if inUnipartite {
		entityIds, err = es.Unipartite.EntityIdsAdjacentTo(entityId)
		if err != nil {
			return []EntityPresence{}, err
		}
	} else {
		entityIds = set.NewSet[string]()
	}

	// Get the entities connected to the entity of interest from the bipartite graph store
	entityIdsFromBipartite := es.entityIdsFromBipartite(entityId)
	if err != nil {
		return []EntityPresence{}, err
	}

	entityIds.AddAll(entityIdsFromBipartite.ToSlice())

	// Determine whether the entities can be found in the unipartite and bipartite graphs
	presence := []EntityPresence{}

	for _, connectionEntityId := range entityIds.ToSlice() {

		connectionInUnipartite, err := es.Unipartite.HasEntity(connectionEntityId)
		if err != nil {
			return []EntityPresence{}, err
		}

		connectionInBipartite, err := es.Bipartite.HasEntityWithId(connectionEntityId)
		if err != nil {
			return []EntityPresence{}, err
		}

		presence = append(presence, EntityPresence{
			EntityId:     connectionEntityId,
			InUnipartite: connectionInUnipartite,
			InBipartite:  connectionInBipartite,
		})
	}

	// Sort the entities by ID
	sort.Slice(presence, func(i, j int) bool {
		return presence[i].EntityId < presence[j].EntityId
	})

	return presence, nil
}

// GetEntity looks for an entity in the bipartite and unipartite stores.
func (es *EntitySearch) GetEntity(entityId string) SearchEntity {

	entity := NewSearchEntity(entityId)

	// Get the entity from the bipartite graph store
	bipartiteEntity, err := es.Bipartite.GetEntity(entityId)
	if err == graphstore.ErrEntityNotFound {
		entity.BipartiteDetails.InBipartite = false

	} else if err != nil {
		// An error occurred
		entity.Error = ErrorDetails{
			ErrorOccurred: true,
			ErrorMessage:  err.Error(),
		}
		return entity

	} else {
		// Entity exists in the bipartite store
		entity.BipartiteDetails, err = es.entityToBipartiteDetails(bipartiteEntity)
		if err != nil {
			entity.Error = ErrorDetails{
				ErrorOccurred: true,
				ErrorMessage:  err.Error(),
			}
			return entity
		}
	}

	entity.InUnipartite, err = es.Unipartite.HasEntity(entityId)
	if err != nil {
		entity.Error = ErrorDetails{
			ErrorOccurred: true,
			ErrorMessage:  err.Error(),
		}
	}

	// Get the linked entities by checking the unipartite and bipartite stores
	entity.LinkedEntities, err = es.linkedEntityPresence(entityId)
	if err != nil {
		entity.Error = ErrorDetails{
			ErrorOccurred: true,
			ErrorMessage:  err.Error(),
		}
	}

	return entity
}
