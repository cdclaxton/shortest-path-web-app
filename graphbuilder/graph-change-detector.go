package graphbuilder

import "github.com/cdclaxton/shortest-path-web-app/filedetector"

// isGraphBuildingRequired checks whether the unipartite and bipartite graphs need constructing
// from the input data.
func isGraphBuildingRequired(config GraphConfig, signatureFilepath string) (
	bool, *filedetector.FileSignatureInfo, error) {

	// Are the bipartite and unipartite graphs backed by Pebble (i.e. persisted)?
	if config.BipartiteConfig.Type != StorageTypePebble ||
		config.UnipartiteConfig.Type != StorageTypePebble {
		return true, nil, nil

	}

	// Get a slice of all of the files to check
	filepaths := filesToCheck(config.Data)

	// Return whether the files have changed
	return filedetector.FilesChanged(filepaths, signatureFilepath)
}

// filesToCheck that are specified in the graph data (i.e. entity, document, link and skip entities
// file).
func filesToCheck(data GraphData) []string {

	// Initialise a slice of files to check
	var numSkipEntities int = 0
	if data.SkipEntitiesFile != "" {
		numSkipEntities = 1
	}

	totalFiles := len(data.DocumentsFiles) + len(data.EntitiesFiles) +
		len(data.LinksFiles) + numSkipEntities
	files := make([]string, totalFiles)

	idx := 0

	// Add the entity files
	for _, entityFile := range data.EntitiesFiles {
		files[idx] = entityFile.Path
		idx += 1
	}

	// Add the document files
	for _, documentFile := range data.DocumentsFiles {
		files[idx] = documentFile.Path
		idx += 1
	}

	// Add the link files
	for _, linkFile := range data.LinksFiles {
		files[idx] = linkFile.Path
		idx += 1
	}

	// Add the skip entities file
	if numSkipEntities != 0 {
		files[idx] = data.SkipEntitiesFile
	}

	return files
}
