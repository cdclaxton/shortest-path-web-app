package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/aymerick/raymond"
	"github.com/cdclaxton/shortest-path-web-app/job"
)

const (
	MinimumNumberHops        = 1                 // Minimum number of hops from an entity to another
	MaximumNumberHops        = 5                 // Maximum number of hops from an entity to another
	MaxDatasetIndex          = 3                 // Maximum number of datasets on the frontend
	NumberHopsInputName      = "numberHops"      // Name of select box for number of hops
	DatasetNameInputName     = "datasetName"     // Prefix of the name of the text box for the dataset name
	DatasetEntitiesInputName = "datasetEntities" // Prefix of the name of the text box containing entity IDs
)

// parseNumberOfHops in the HTTP POST form  data.
func parseNumberOfHops(req *http.Request) (int, error) {

	// Read the number of hops from the form
	numberHops := req.FormValue(NumberHopsInputName)

	if len(numberHops) == 0 {
		return 0, fmt.Errorf("Number of hops is blank")
	}

	// Convert the string version of the number of hops to an integer
	value, err := strconv.Atoi(numberHops)
	if err != nil {
		return 0, fmt.Errorf("Invalid number of hops: %v", value)
	}

	// Validate the number of hops
	if value < MinimumNumberHops || value > MaximumNumberHops {
		return 0, fmt.Errorf("Invalid number of hops: %v", value)
	}

	return value, nil
}

// splitEntityIDs from a string using space, newline, comma and semicolon separators.
func splitEntityIDs(text string) []string {

	// Split the potential entity IDs from the string
	re := regexp.MustCompile("[ ,;\t\n]+")
	potentialEntityIds := re.Split(text, -1)

	// Retain entity IDs that pass basic validation
	entityIds := []string{}
	for idx := range potentialEntityIds {
		if len(potentialEntityIds[idx]) > 0 {
			entityIds = append(entityIds, potentialEntityIds[idx])
		}
	}

	return entityIds
}

// Errors that can occur with user-defined datasets
var (
	DatasetErrorNoName     = fmt.Errorf("Dataset has no name")
	DatasetErrorNoEntities = fmt.Errorf("Dataset has no entity IDs")
)

// parseEntitySet from the HTTP POST form data.
func parseEntitySet(req *http.Request, index int) (*job.EntitySet, error) {

	// Preconditions
	if req == nil {
		return nil, fmt.Errorf("HTTP request is nil")
	}

	if index < 0 || index > MaxDatasetIndex {
		return nil, fmt.Errorf("Invalid dataset index: %v", index)
	}

	// Extract the (user-friendly) name of the dataset from the form
	name := req.FormValue(DatasetNameInputName + strconv.Itoa(index))

	// Extract the entity IDs from the form
	allEntityIds := req.FormValue(DatasetEntitiesInputName + strconv.Itoa(index))
	entityIds := splitEntityIDs(allEntityIds)

	// Determine if the dataset passes minimum validity tests
	hasName := len(name) > 0
	hasEntityIds := len(entityIds) > 0

	if hasName && hasEntityIds {
		return &job.EntitySet{
			Name:      name,
			EntityIds: entityIds,
		}, nil
	} else if hasName && !hasEntityIds {
		return nil, DatasetErrorNoEntities
	} else if !hasName && hasEntityIds {
		return nil, DatasetErrorNoName
	} else {
		return nil, nil
	}
}

// extractJobConfigurationFromForm extracts, parses and validates the configuration for a job.
func extractJobConfigurationFromForm(req *http.Request, maxDatasetIndex int) (*job.JobConfiguration, error) {

	// Preconditions
	if req == nil {
		return nil, fmt.Errorf("HTTP request is nil")
	}

	if err := req.ParseForm(); err != nil {
		return nil, fmt.Errorf("Unable to parse form: %v", err)
	}

	// Parse the number of hops
	numberHops, err := parseNumberOfHops(req)
	if err != nil {
		return nil, fmt.Errorf("Invalid number of hops: %v", err)
	}

	// Initialise the job configuration
	jobConf := job.JobConfiguration{
		MaxNumberHops: numberHops,
		EntitySets:    []job.EntitySet{},
	}

	// Parse the datasets
	for idx := 1; idx <= maxDatasetIndex; idx++ {
		entitySet, err := parseEntitySet(req, idx)

		if err != nil {
			return nil, fmt.Errorf("Dataset parse error: %v", err)
		}

		if entitySet != nil {
			jobConf.EntitySets = append(jobConf.EntitySets, *entitySet)
		}
	}

	if len(jobConf.EntitySets) == 0 {
		return nil, fmt.Errorf("There are no datasets")
	}

	return &jobConf, nil
}

func upload(w http.ResponseWriter, req *http.Request) {

	// Extract the data from the form
	jobConf, err := extractJobConfigurationFromForm(req, MaxDatasetIndex)

	// If there was an input configuration error, then show the error on a dedicated page
	if err != nil {
		template, err2 := raymond.ParseFile("./server/templates/input-problem.html")
		if err2 != nil {
			fmt.Fprintf(w, "HTML template not found. Data error: %v\n", err)
		}

		result, err3 := template.Exec(map[string]string{
			"reason": err.Error(),
		})

		if err3 != nil {
			fmt.Fprintf(w, "HTML template not usable. Data error: %v\n", err)
		}

		fmt.Fprintf(w, result)
		return
	}

	// Launch the job
	fmt.Fprintf(w, "%v", jobConf)
}

func main() {
	fmt.Println("Hello!")

	// Static content
	http.Handle("/", http.FileServer(http.Dir("./server/static/")))

	// Uploading job configuration
	http.HandleFunc("/upload", upload)

	http.ListenAndServe(":8090", nil)
}
