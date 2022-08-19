package job

import "time"

// An EntitySet represents a named group of entity IDs.
type EntitySet struct {
	EntityIds []string // Entity IDs linked to the Name
	Name      string   // Name, e.g. data source name, tasking name
}

// A Tasking is the reason for the user to use the web-app.
type Tasking struct {
	Identifier string // Unique tasking reference
	User       string // User of the web-app
}

// An OutputType represents the type of output to return.
type OutputType string

const (
	JsonOutput OutputType = "JSON"
	I2Output   OutputType = "i2"
	CsvOutput  OutputType = "CSV"
)

// JobConfiguration specifies all of the necessary details of the job.
type JobConfiguration struct {
	EntitySets         []EntitySet // Sets of entities from which to find paths
	MaxNumberHops      int         // Number of steps from a root to a goal to search
	TaskingInformation Tasking     // Tasking information
	RequiredOutput     OutputType  // Type of output to return
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
