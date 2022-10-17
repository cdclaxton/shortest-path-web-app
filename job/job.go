package job

import "time"

// An EntitySet represents a named group of entity IDs.
type EntitySet struct {
	Name      string   // Name, e.g. data source name, tasking name
	EntityIds []string // Entity IDs linked to the Name
}

// JobConfiguration specifies all of the necessary details of the job.
type JobConfiguration struct {
	MaxNumberHops int         // Number of steps from a root to a goal to search
	EntitySets    []EntitySet // Sets of entities from which to find paths
}

// A JobStatus represents the current state of the job.
type JobStatus string

const (
	NotStarted JobStatus = "Not started"
	InProgress JobStatus = "In progress"
	Failed     JobStatus = "Failed"
	Complete   JobStatus = "Complete"
)

// JobProgress records salient information about the job's status and timing.
type JobProgress struct {
	Status    JobStatus
	StartTime time.Time
	EndTime   time.Time
}

type Job struct {
	GUID          string           // Unique ID for the job
	Configuration JobConfiguration // Configuration, i.e. what job to perform
	Progress      JobProgress      // Progress of the job
}
