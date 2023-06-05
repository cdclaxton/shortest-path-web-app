package graphloader

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/logging"
)

const componentName = "graphLoader"

var (
	ErrInvalidNumberEntityWorkers   = errors.New("invalid number of entity file workers")
	ErrInvalidNumberDocumentWorkers = errors.New("invalid number of document file workers")
	ErrInvalidNumberLinkWorkers     = errors.New("invalid number of link file workers")
	ErrEmptyDelimiter               = errors.New("empty delimiter")
	ErrInvalidDelimiter             = errors.New("invalid delimiter")
)

// A GraphStoreLoaderFromCsv loads a bipartite graph store from entity, document and link CSV files.
type GraphStoreLoaderFromCsv struct {
	graphStore         graphstore.BipartiteGraphStore
	entityFiles        []EntitiesCsvFile
	documentFiles      []DocumentsCsvFile
	linkFiles          []LinksCsvFile
	ignoreInvalidLinks bool // Ignore links that cannot be created, e.g. due to missing entity or document
	numEntityWorkers   int  // Number of entity file workers
	numDocumentWorkers int  // Number of document file workers
	numLinkWorkers     int  // Number of link file workers
}

// NewGraphStoreLoaderFromCsv constructs a graph store loader that reads CSV files.
func NewGraphStoreLoaderFromCsv(graphStore graphstore.BipartiteGraphStore,
	entityFiles []EntitiesCsvFile,
	documentFiles []DocumentsCsvFile,
	linkFiles []LinksCsvFile,
	ignoreInvalidLinks bool,
	numEntityWorkers int, numDocumentWorkers int, numLinkWorkers int) *GraphStoreLoaderFromCsv {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfEntityFiles", strconv.Itoa(len(entityFiles))).
		Str("numberOfDocumentFiles", strconv.Itoa(len(documentFiles))).
		Str("numberOfLinksFiles", strconv.Itoa(len(linkFiles))).
		Str("ignoreInvalidLinks", strconv.FormatBool(ignoreInvalidLinks)).
		Str("numberOfEntityWorkers", strconv.Itoa(numEntityWorkers)).
		Str("numberOfDocumentWorkers", strconv.Itoa(numDocumentWorkers)).
		Str("numberOfLinkWorkers", strconv.Itoa(numLinkWorkers)).
		Msg("Creating a bipartite graph store loader")

	return &GraphStoreLoaderFromCsv{
		graphStore:         graphStore,
		entityFiles:        entityFiles,
		documentFiles:      documentFiles,
		linkFiles:          linkFiles,
		ignoreInvalidLinks: ignoreInvalidLinks,
		numEntityWorkers:   numEntityWorkers,
		numDocumentWorkers: numDocumentWorkers,
		numLinkWorkers:     numLinkWorkers,
	}
}

// Load the bipartite graph store from CSV files.
func (loader *GraphStoreLoaderFromCsv) Load() error {

	// Check the number of workers
	if loader.numEntityWorkers <= 0 {
		return ErrInvalidNumberEntityWorkers
	}

	if loader.numDocumentWorkers <= 0 {
		return ErrInvalidNumberDocumentWorkers
	}

	if loader.numLinkWorkers <= 0 {
		return ErrInvalidNumberLinkWorkers
	}

	// Make a context that allows workers to detect that all jobs must cease
	ctx := context.Background()
	ctx, cancelCtx := context.WithCancel(ctx)

	// Put the entity files to load on a channel
	entityFilesChan := entityFilesChannel(loader.entityFiles)
	close(entityFilesChan)

	// Put the document files to load onto a channel
	documentFilesChan := documentFilesChannel(loader.documentFiles)
	close(documentFilesChan)

	// Put the links files to load onto a channel
	linkFileChan := linkFilesChannel(loader.linkFiles)
	close(linkFileChan)

	// Make a channel to hold errors from the goroutines. The worse case situation is that
	// every worker fails simultaneously, so a buffered channel is required
	errChan := make(chan error, loader.numEntityWorkers+loader.numDocumentWorkers+loader.numLinkWorkers)

	var wg sync.WaitGroup

	// Run the entity file loader workers
	for i := 0; i < loader.numEntityWorkers; i++ {
		wg.Add(1)
		go entityWorker(ctx, cancelCtx, i, entityFilesChan, errChan, &wg, loader.graphStore)
	}

	// Run the document file loader workers
	for i := 0; i < loader.numDocumentWorkers; i++ {
		wg.Add(1)
		go documentWorker(ctx, cancelCtx, i, documentFilesChan, errChan, &wg, loader.graphStore)
	}

	// Wait until all the entity and document workers have completed
	wg.Wait()

	// Extract the first error from the error channel
	err := takeFirstErrorFromChannel(errChan)
	if err != nil {
		return err
	}

	// Run the link file loader workers
	for i := 0; i < loader.numLinkWorkers; i++ {
		wg.Add(1)
		go linkWorker(ctx, cancelCtx, i, linkFileChan, errChan, &wg, loader.graphStore, loader.ignoreInvalidLinks)
	}

	// Wait until the link workers have completed
	wg.Wait()

	// Extract the first error from the error channel
	return takeFirstErrorFromChannel(errChan)
}

// takeFirstErrorFromChannel returns the first error from the error channel.
func takeFirstErrorFromChannel(errChan <-chan error) error {
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// entityFilesChannel creates a populated, buffered channel of entity files.
func entityFilesChannel(files []EntitiesCsvFile) chan EntitiesCsvFile {
	c := make(chan EntitiesCsvFile, len(files))

	for _, file := range files {
		c <- file
	}

	return c
}

// documentFilesChannel creates a populated, buffered channel of document files.
func documentFilesChannel(files []DocumentsCsvFile) chan DocumentsCsvFile {
	c := make(chan DocumentsCsvFile, len(files))

	for _, file := range files {
		c <- file
	}

	return c
}

// linkFilesChannel creates a populated, buffered channel of links files.
func linkFilesChannel(files []LinksCsvFile) chan LinksCsvFile {
	c := make(chan LinksCsvFile, len(files))

	for _, file := range files {
		c <- file
	}

	return c
}

// loadEntitiesFromFile loads the entities in the CSV file into the bipartite graph store.
func loadEntitiesFromFile(entityFile EntitiesCsvFile, graphStore graphstore.BipartiteGraphStore) error {

	// Create an entities CSV file reader
	reader := NewEntitiesCsvFileReader(entityFile)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has entities to read, add the entities to the graph store
	for reader.hasNext {
		entity, err := reader.Next()

		if err != nil {
			return err
		}

		if err := graphStore.AddEntity(entity); err != nil {
			return err
		}
	}

	return reader.Close()
}

// entityWorker is a worker that receives entity file jobs to run.
func entityWorker(ctx context.Context, cancelCtx context.CancelFunc, workerIdx int,
	entityFilesChan <-chan EntitiesCsvFile, errChan chan<- error,
	wg *sync.WaitGroup, graphStore graphstore.BipartiteGraphStore) {

	defer wg.Done()

	for entityFile := range entityFilesChan {

		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("entity worker", strconv.Itoa(workerIdx)).
			Str("filepath", entityFile.Path).
			Msg("Entity file job received by worker")

		// Check to see if the worker should prematurely end
		select {
		case <-ctx.Done():
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("entity worker", strconv.Itoa(workerIdx)).
				Msg("Entity worker shutting down")
			return
		default:
		}

		err := loadEntitiesFromFile(entityFile, graphStore)
		if err != nil {
			logging.Logger.Error().
				Str(logging.ComponentField, componentName).
				Str("entity worker", strconv.Itoa(workerIdx)).
				Err(err).
				Msg("Entity worker has encountered an error")
			errChan <- err
			cancelCtx()
		}
	}
}

// loadDocumentsFromFile loads the documents in the CSV file into the bipartite graph store.
func loadDocumentsFromFile(documentFile DocumentsCsvFile, graphStore graphstore.BipartiteGraphStore) error {

	// Create a documents CSV file reader
	reader := NewDocumentsCsvFileReader(documentFile)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has documents to read, add the documents to the graph store
	for reader.hasNext {
		document, err := reader.Next()

		if err != nil {
			return err
		}

		if err := graphStore.AddDocument(document); err != nil {
			return err
		}
	}

	return reader.Close()
}

// documentWorker is a worker that receives document file jobs to run.
func documentWorker(ctx context.Context, cancelCtx context.CancelFunc, workerIdx int,
	documentFilesChan <-chan DocumentsCsvFile, errChan chan<- error,
	wg *sync.WaitGroup, graphStore graphstore.BipartiteGraphStore) {

	defer wg.Done()

	for documentFile := range documentFilesChan {

		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("document worker", strconv.Itoa(workerIdx)).
			Str("filepath", documentFile.Path).
			Msg("Document file job received by worker")

		// Check to see if the worker should prematurely end
		select {
		case <-ctx.Done():
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("document worker", strconv.Itoa(workerIdx)).
				Msg("Document worker shutting down")
			return
		default:
		}

		err := loadDocumentsFromFile(documentFile, graphStore)
		if err != nil {
			errChan <- err
			cancelCtx()
		}
	}
}

// loadLinksFromFile loads the links in the CSV file into the bipartite graph store.
func loadLinksFromFile(linkFile LinksCsvFile, graphStore graphstore.BipartiteGraphStore,
	ignoreInvalidLinks bool) error {

	// Create a links CSV file reader
	reader := NewLinksCsvFileReader(linkFile)

	// Initialise the CSV reader
	err := reader.Initialise()
	if err != nil {
		return err
	}

	// While the file has links to read, add the links to the graph store
	for reader.hasNext {
		link, err := reader.Next()

		if err != nil {
			return err
		}

		// Try to add the link
		err = graphStore.AddLink(link)

		// If there is an error, handle it if required
		if err != nil {
			if !ignoreInvalidLinks {
				return err
			} else {
				if err != graphstore.ErrEntityNotFound && err != graphstore.ErrDocumentNotFound {
					return err
				}

				logging.Logger.Info().
					Str(logging.ComponentField, componentName).
					Str("filepath", linkFile.Path).
					Str("entityId", link.EntityId).
					Str("documentId", link.DocumentId).
					Str("message", err.Error()).
					Msg("Gracefully handling error with link")
			}
		}
	}

	return nil
}

// linkWorker is a worker that receives link file jobs to run.
func linkWorker(ctx context.Context, cancelCtx context.CancelFunc, workerIdx int,
	linkFilesChan <-chan LinksCsvFile, errChan chan<- error,
	wg *sync.WaitGroup, graphStore graphstore.BipartiteGraphStore,
	ignoreInvalidLinks bool) {

	defer wg.Done()

	for linkFile := range linkFilesChan {

		logging.Logger.Info().
			Str(logging.ComponentField, componentName).
			Str("link worker", strconv.Itoa(workerIdx)).
			Str("filepath", linkFile.Path).
			Msg("Link file job received by worker")

		// Check to see if the worker should prematurely end
		select {
		case <-ctx.Done():
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("link worker", strconv.Itoa(workerIdx)).
				Msg("Link worker shutting down")
			return
		default:
		}

		err := loadLinksFromFile(linkFile, graphStore, ignoreInvalidLinks)
		if err != nil {
			errChan <- err
			cancelCtx()
		}
	}
}

// findIndicesOfFields returns a mapping of the field name to index.
func findIndicesOfFields(header []string, fields []string) (map[string]int, error) {

	// Map containing all the fields present in the header
	allFields := map[string]int{}

	for idx, name := range header {
		allFields[name] = idx
	}

	// Create a mapping for the fields that are required
	needed := map[string]int{}
	missingFields := []string{}

	for _, field := range fields {
		index, found := allFields[field]
		if !found {
			missingFields = append(missingFields, field)
		} else {
			needed[field] = index
		}
	}

	// Return an error message if any of the fields are missing
	if len(missingFields) != 0 {
		return needed, fmt.Errorf("header has missing field(s): %v",
			strings.Join(missingFields, ","))
	}

	return needed, nil
}

// attributeToFieldIndex creates a mapping from the attribute name to the field index.
func attributeToFieldIndex(header []string, fieldToAttribute map[string]string) (
	map[string]int, error) {

	// Slice of field names
	fieldNames := []string{}
	for field := range fieldToAttribute {
		fieldNames = append(fieldNames, field)
	}

	// Find the attribute field indices
	fieldToIndex, err := findIndicesOfFields(header, fieldNames)

	if err != nil {
		return nil, err
	}

	// Map of attribute name to field index
	attributeFieldIndex := map[string]int{}

	for _, field := range fieldNames {
		attributeName := fieldToAttribute[field]
		attributeFieldIndex[attributeName] = fieldToIndex[field]
	}

	return attributeFieldIndex, nil
}

// extractAttributes from a row of data given the mapping from the attribute name to field index.
func extractAttributes(row []string, attributeToFieldIndex map[string]int) (
	map[string]string, error) {

	// Map of attribute name to its value
	attributes := map[string]string{}

	for attributeName, fieldIndex := range attributeToFieldIndex {

		// Check the field index is valid
		if fieldIndex < 0 || fieldIndex >= len(row) {
			return nil, fmt.Errorf("invalid field index: %v", fieldIndex)
		}

		attributes[attributeName] = row[fieldIndex]
	}

	return attributes, nil
}

// parseDelimiter to use when reading CSV files.
func parseDelimiter(delimiter string) (rune, error) {

	// Preconditions
	if len(delimiter) == 0 {
		return rune(0), ErrEmptyDelimiter
	}

	if len(delimiter) > 1 {
		return rune(0), ErrInvalidDelimiter
	}

	// Return the rune
	return rune(delimiter[0]), nil
}
