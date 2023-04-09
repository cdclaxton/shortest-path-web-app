package job

import (
	"errors"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

var (
	ErrInvalidNumberSteps = errors.New("invalid number of steps")
	ErrNoSeedEntities     = errors.New("no seed entities")
	ErrSeedEntitiesIsNil  = errors.New("seed entities is nil")
	ErrConfigIsNil        = errors.New("spider config is nil")
)

// SpiderJobConfiguration holds the data for running spidering.
type SpiderJobConfiguration struct {
	NumberSteps  int              // Number of steps from the seed entities
	SeedEntities *set.Set[string] // Seed entities
}

// isValid returns an error if the spider job configuration is invalid.
func (s *SpiderJobConfiguration) isValid() error {

	// Check the number of steps
	if s.NumberSteps < 0 {
		return ErrInvalidNumberSteps
	}

	if s.SeedEntities == nil {
		return ErrSeedEntitiesIsNil
	}

	// Check there are seed entities and that each entity ID is valid
	if s.SeedEntities.Len() == 0 {
		return ErrNoSeedEntities
	}

	for _, entityId := range s.SeedEntities.ToSlice() {
		err := graphstore.ValidateEntityId(entityId)
		if err != nil {
			return err
		}
	}

	return nil
}

// NewSpiderJobConfiguration constructs a new spider job configuration and ensures
// the data is valid.
func NewSpiderJobConfiguration(numberSteps int, seedEntities *set.Set[string]) (
	*SpiderJobConfiguration, error) {

	conf := SpiderJobConfiguration{
		NumberSteps:  numberSteps,
		SeedEntities: seedEntities,
	}

	// Check the configuration is valid
	if err := conf.isValid(); err != nil {
		return nil, err
	}

	return &conf, nil
}

type SpiderJob struct {
	GUID          string                  // Unique ID for the job
	Configuration *SpiderJobConfiguration // Configuration
	Progress      JobProgress             // Progress of the job
	ResultFile    string                  // Location of the result file for download
	Message       string                  // Message to present to the user
	Error         error                   // Error (if one occurs during processing of the job)
}

// NewSpiderJob creates a new spidering job.
func NewSpiderJob(conf *SpiderJobConfiguration) (SpiderJob, error) {

	// Check the job configuration is valid
	if conf == nil {
		return SpiderJob{}, ErrConfigIsNil
	}

	if err := conf.isValid(); err != nil {
		return SpiderJob{}, err
	}

	return SpiderJob{
		GUID:          generateGuid(),
		Configuration: conf,
		Progress:      NewJobProgress(),
	}, nil
}

// HasValidGuid returns true if the GUID is deemed valid.
func (j *SpiderJob) HasValidGuid() bool {
	return len(j.GUID) == 36
}
