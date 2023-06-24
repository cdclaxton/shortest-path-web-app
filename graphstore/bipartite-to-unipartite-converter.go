package graphstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

var (
	ErrBipartiteStoreIsNil    = errors.New("bipartite store is nil")
	ErrUnipartiteStoreIsNil   = errors.New("unipartite store is nil")
	ErrEntitiesToSkipIsNil    = errors.New("entities to skip is nil")
	ErrInvalidNumberOfWorkers = errors.New("invalid number of workers")
	ErrInvalidJobChannelSize  = errors.New("invalid job chnanel size")
)

// BipartiteToUnipartite converter to load a unipartite graph from a bipartite graph.
//
// The set of skipEntities are those entities that won't be transferred to the unipartite graph.
func BipartiteToUnipartite(bi BipartiteGraphStore, uni UnipartiteGraphStore,
	skipEntities *set.Set[string], numWorkers int, jobChannelSize int) error {

	// Preconditions
	if bi == nil {
		return ErrBipartiteStoreIsNil
	}

	if uni == nil {
		return ErrUnipartiteStoreIsNil
	}

	if skipEntities == nil {
		return ErrEntitiesToSkipIsNil
	}

	if numWorkers < 1 {
		return fmt.Errorf("%w: %d", ErrInvalidNumberOfWorkers, numWorkers)
	}

	if jobChannelSize < 1 {
		return fmt.Errorf("%w: %d", ErrInvalidJobChannelSize, jobChannelSize)
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("numberOfWorkers", strconv.Itoa(numWorkers)).
		Str("jobChannelSize", strconv.Itoa(jobChannelSize)).
		Msg("Starting bipartite to unipartite conversion")

	// Buffered channel on which to place jobs (i.e. documents to process)
	jobsChan := make(chan conversionJob, jobChannelSize)

	// Channel to hold errors from the generator and workers
	errChan := make(chan error, numWorkers+1)

	var wg sync.WaitGroup
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)

	// Start the document generator
	wg.Add(1)
	go documentGenerator(&wg, ctx, cancelFunc, bi, jobsChan, errChan)

	// Start the workers
	for workerIdx := 0; workerIdx < numWorkers; workerIdx++ {
		wg.Add(1)
		go conversionWorker(workerIdx, &wg, ctx, cancelFunc, jobsChan, errChan, bi, uni, skipEntities)
	}

	// Wait for the document generator and workers to finish
	wg.Wait()

	// Check to see if an error occurred
	select {
	case msg := <-errChan:
		return msg
	default:
	}

	err := uni.Finalise()
	if err != nil {
		return err
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Finished bipartite to unipartite conversion")

	return nil
}

type conversionJob struct {
	documentId       string
	documentIndex    int
	numDocsToProcess int
}

// documentGenerator places document IDs from the bipartite store onto a job channel for workers.
func documentGenerator(wg *sync.WaitGroup, ctx context.Context, cancelCtx context.CancelFunc,
	bi BipartiteGraphStore, jobChannel chan<- conversionJob, errChan chan<- error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Starting document generator for bipartite to unipartite conversion")

	defer wg.Done()
	defer close(jobChannel)

	// Get the total number of documents to process
	totalDocs, err := bi.NumberOfDocuments()
	if err != nil {
		errChan <- err
		cancelCtx()
		return
	}

	// Iterator to retrieve documents from the bipartite graph store
	it, err := bi.NewDocumentIdIterator()
	if err != nil {
		errChan <- err
		cancelCtx()
		return
	}

	docIndex := 0
	for it.hasNext() {

		// Check to see if the generation should prematurely end
		select {
		case <-ctx.Done():
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Msg("Document generator received cancel notification")
			return
		default:
		}

		docIndex += 1

		// Get the next document ID from the iterator
		docId, err := it.nextDocumentId()
		if err != nil {
			errChan <- err
			cancelCtx()
			return
		}

		jobChannel <- conversionJob{
			documentId:       docId,
			documentIndex:    docIndex,
			numDocsToProcess: totalDocs,
		}
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Msg("Closing down document generator for bipartite to unipartite conversion")
}

// conversionWorker receives jobs from a channel and creates links in the unipartite store.
func conversionWorker(workerIdx int, wg *sync.WaitGroup, ctx context.Context,
	cancelCtx context.CancelFunc, jobChannel <-chan conversionJob, errChan chan<- error,
	bi BipartiteGraphStore, uni UnipartiteGraphStore, skipEntities *set.Set[string]) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("workerIndex", strconv.Itoa(workerIdx)).
		Msg("Starting bipartite to unipartite conversion worker")

	defer wg.Done()
	numJobsProcessed := 0

	for job := range jobChannel {

		// Check to see if the conversion should prematurely end
		select {
		case <-ctx.Done():
			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("workerIndex", strconv.Itoa(workerIdx)).
				Msg("Conversion worker received cancel notification")
			return
		default:
		}

		// Log progress
		if job.documentIndex%100000 == 0 {
			percentageComplete := (1.0 * float64(job.documentIndex)) / (float64(job.numDocsToProcess) * 1.0) * 100.0

			logging.Logger.Info().
				Str(logging.ComponentField, componentName).
				Str("numberDocsRead", fmt.Sprint(job.documentIndex)).
				Str("totalDocsToRead", fmt.Sprint(job.numDocsToProcess)).
				Str("percentageComplete", fmt.Sprint(percentageComplete)).
				Msg("Building unipartite graph")
		}

		// Get the document given its ID
		doc, err := bi.GetDocument(job.documentId)
		if err != nil {
			errChan <- err
			cancelCtx()
			return
		}
		if doc == nil {
			errChan <- fmt.Errorf("document doesn't exist with ID: %v", job.documentId)
			cancelCtx()
			return
		}

		// If there is just a single entity, add it to the graph
		if doc.LinkedEntityIds.Len() == 1 {
			for entityId := range doc.LinkedEntityIds.Values {
				uni.AddEntity(entityId)
			}
			continue
		}

		// Add the entities to the graph
		for e1 := range doc.LinkedEntityIds.Values {

			if skipEntities.Has(e1) {
				continue
			}

			for e2 := range doc.LinkedEntityIds.Values {

				if !skipEntities.Has(e2) && e1 != e2 {
					// Add the link
					err := uni.AddUndirected(e1, e2)
					if err != nil {
						errChan <- err
						cancelCtx()
						return
					}
				}
			}
		}

		numJobsProcessed += 1
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Int("workerIndex", workerIdx).
		Int("numJobsProcessed", numJobsProcessed).
		Msg("Closing down bipartite to unipartite conversion worker")
}
