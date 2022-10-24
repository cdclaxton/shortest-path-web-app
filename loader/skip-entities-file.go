package loader

import (
	"bufio"
	"os"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// ReadSkipEntities from a simple text file.
func ReadSkipEntities(filepath string) (*set.Set[string], error) {

	logging.Logger.Info().
		Str("Filepath", filepath).
		Msg("Reading skip entities CSV file")

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	// Ensure the file is closed
	defer file.Close()

	// Read the file line-by-line using a scanner
	scanner := bufio.NewScanner(file)

	// Set of entities
	entities := set.NewSet[string]()

	// Walk through each line in the file
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			entities.Add(line)
		}
	}

	return entities, nil
}
