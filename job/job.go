package job

import (
	"fmt"
	"strings"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/google/uuid"
)

// An EntitySet represents a named group of entity IDs.
type EntitySet struct {
	Name      string   // Name, e.g. data source name, tasking name
	EntityIds []string // Entity IDs linked to the Name
}

var (
	ErrEntitySetNoName      = fmt.Errorf("Entity set doesn't have a name")
	ErrEntitySetNoEntityIDs = fmt.Errorf("Entity set doesn't have any entity IDs")
	ErrInvalidNumberOfHops  = fmt.Errorf("Invalid number of hops")
	ErrNoEntitySets         = fmt.Errorf("No entity sets")
)

// Validate the EntitySet.
func (e *EntitySet) Validate() error {

	// Check the name of the entity set
	if len(strings.TrimSpace(e.Name)) == 0 {
		return ErrEntitySetNoName
	}

	// Check that there are entity IDs
	if len(e.EntityIds) == 0 {
		return ErrEntitySetNoEntityIDs
	}

	// Check each entity ID
	for _, entityId := range e.EntityIds {
		err := graphstore.ValidateEntityId(entityId)
		if err != nil {
			return err
		}
	}

	return nil
}

// JobConfiguration specifies all of the necessary details of the job.
type JobConfiguration struct {
	MaxNumberHops int         // Number of steps from a root to a goal to search
	EntitySets    []EntitySet // Sets of entities from which to find paths
}

// NewJobConfiguration given the entitySets to find paths between and the number of hops.
func NewJobConfiguration(entitySets []EntitySet, numberHops int) (*JobConfiguration, error) {

	j := JobConfiguration{
		EntitySets:    entitySets,
		MaxNumberHops: numberHops,
	}

	// Postconditions
	if err := j.Validate(); err != nil {
		return nil, err
	}

	return &j, nil
}

// Validate the job configuration.
func (j *JobConfiguration) Validate() error {

	if j.MaxNumberHops < 1 {
		return ErrInvalidNumberOfHops
	}

	if len(j.EntitySets) == 0 {
		return ErrNoEntitySets
	}

	for _, entitySet := range j.EntitySets {
		err := entitySet.Validate()
		if err != nil {
			return err
		}
	}

	return nil
}

// A JobState represents the current state of the job.
type JobState string

const (
	NotStarted        JobState = "Not started"
	InProgress        JobState = "In progress"
	Failed            JobState = "Failed"
	CompleteResults   JobState = "Complete Results"
	CompleteNoResults JobState = "Complete No Results"
)

// JobProgress records salient information about the job's status and timing.
type JobProgress struct {
	State     JobState
	StartTime time.Time
	EndTime   time.Time
}

func NewJobProgress() JobProgress {
	return JobProgress{
		State:     NotStarted,
		StartTime: time.Time{},
		EndTime:   time.Time{},
	}
}

type Job struct {
	GUID          string            // Unique ID for the job
	Configuration *JobConfiguration // Configuration, i.e. what job to perform
	Progress      JobProgress       // Progress of the job
	ResultFile    string            // Location of the result file for download
	Message       string            // Message to present to the user
	Error         error             // Error (if one occurs during processing of the job)
}

// GenerateGuid generates a GUID for the job identifier.
func generateGuid() string {
	return uuid.New().String()
}

func NewJob(conf *JobConfiguration) (Job, error) {

	// Preconditions
	err := conf.Validate()
	if err != nil {
		return Job{}, err
	}

	return Job{
		GUID:          generateGuid(),
		Configuration: conf,
		Progress:      NewJobProgress(),
		ResultFile:    "",
	}, nil
}

// HasValidGuid returns true if the GUID is deemed valid.
func (j *Job) HasValidGuid() bool {
	return len(j.GUID) == 36
}
