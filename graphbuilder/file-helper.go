package graphbuilder

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// clearFolder by removing all of its contents.
func clearFolder(folder string) error {

	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, f := range files {

		fmt.Println(filepath.Join(folder, f.Name()))
		os.RemoveAll(filepath.Join(folder, f.Name()))
	}

	return nil
}

func isFolderEmpty(folder string) (bool, error) {
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return false, err
	}

	return len(files) == 0, nil
}
