package loader

type EntitiesCsvFile struct {
	Path             string            // Location of the file
	EntityType       string            // Type of entities in the file
	Delimiter        string            // Delimiter
	IsEncapsulated   bool              // Is an encapsulator used?
	Encapsulator     string            // Encapsulator (if used)
	EntityIdField    string            // Name of the field with the entity ID
	FieldToAttribute map[string]string // Mapping of field name to attribute
}
