package graphstore

// Link represents that an entity ID was found in a document with a given ID.
type Link struct {
	EntityId   string
	DocumentId string
}

func NewLink(entityId string, documentId string) Link {
	return Link{
		EntityId:   entityId,
		DocumentId: documentId,
	}
}
