package loader

type DocumentsCsvFile struct {
	Path             string            // Location of the file
	EntityType       string            // Type of documents in the file
	Delimiter        string            // Delimiter
	IsEncapsulated   bool              // Is an encapsulator used?
	Encapsulator     string            // Encapsulator (if used)
	DocumentIdField  string            // Name of the field with the document ID
	FieldToAttribute map[string]string // Mapping of field name to attribute
}
