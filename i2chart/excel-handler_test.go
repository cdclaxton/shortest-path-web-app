package i2chart

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnIndexToLetter(t *testing.T) {
	testCases := []struct {
		index          int
		expectedLetter string
		expectedError  bool
	}{
		{
			index:          0,
			expectedLetter: "A",
			expectedError:  false,
		},
		{
			index:          1,
			expectedLetter: "B",
			expectedError:  false,
		},
		{
			index:          25,
			expectedLetter: "Z",
			expectedError:  false,
		},
		{
			index:          -1,
			expectedLetter: "",
			expectedError:  true,
		},
	}

	for _, testCase := range testCases {
		actual, err := columnIndexToLetter(testCase.index)
		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedLetter, actual)
	}
}

func TestExcelCellIndex(t *testing.T) {
	testCases := []struct {
		columnIndex   int
		rowIndex      int
		expectedCell  string
		expectedError bool
	}{
		{
			columnIndex:   0,
			rowIndex:      0,
			expectedCell:  "A1",
			expectedError: false,
		},
		{
			columnIndex:   1,
			rowIndex:      0,
			expectedCell:  "B1",
			expectedError: false,
		},
		{
			columnIndex:   0,
			rowIndex:      1,
			expectedCell:  "A2",
			expectedError: false,
		},
		{
			columnIndex:   4,
			rowIndex:      2,
			expectedCell:  "E3",
			expectedError: false,
		},
		{
			columnIndex:   -1,
			rowIndex:      2,
			expectedCell:  "",
			expectedError: true,
		},
		{
			columnIndex:   1,
			rowIndex:      -1,
			expectedCell:  "",
			expectedError: true,
		},
	}

	for _, testCase := range testCases {
		actual, err := excelCellIndex(testCase.columnIndex, testCase.rowIndex)
		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedCell, actual)
	}

}

func TestReadFromExcel(t *testing.T) {
	testCases := []struct {
		filepath      string
		sheetName     string
		expectedRows  [][]string
		expectedError bool
	}{
		{
			// Non-existent file
			filepath:      "./excel-test-data/does-not-exist.xlsx",
			sheetName:     "Sheet1",
			expectedRows:  nil,
			expectedError: true,
		},
		{
			// One cell populated
			filepath:      "./excel-test-data/test1.xlsx",
			sheetName:     "Sheet1",
			expectedRows:  [][]string{{"A1"}},
			expectedError: false,
		},
		{
			// Six cells populated
			filepath:  "./excel-test-data/test2.xlsx",
			sheetName: "Sheet1",
			expectedRows: [][]string{
				{"A1", "B1", "C1"},
				{"A2", "B2", "C2"},
			},
			expectedError: false,
		},
		{
			// Blank row
			filepath:  "./excel-test-data/test3.xlsx",
			sheetName: "Sheet1",
			expectedRows: [][]string{
				{"A1", "B1"},
				nil,
				{"A3", "B3"},
			},
			expectedError: false,
		},
	}

	for _, testCase := range testCases {
		actual, err := ReadFromExcel(testCase.filepath, testCase.sheetName)

		if testCase.expectedError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, testCase.expectedRows, actual)
	}
}

func TestWriteToExcel(t *testing.T) {

	dir, err := ioutil.TempDir("", "test-excel-writer")
	assert.NoError(t, err)

	fmt.Println(dir)

	testCases := []struct {
		filename string
		rows     [][]string
	}{
		{
			filename: "test-1.xlsx",
			rows: [][]string{
				{"CellA1", "CellB1"},
				{"CellA2", "CellB2"},
			},
		},
		{
			filename: "test-2.xlsx",
			rows: [][]string{
				{"CellA1", "CellB1", "CellC1"},
				{"CellA2", "CellB2", "CellC2"},
			},
		},
	}

	for _, testCase := range testCases {

		// Write the Excel file
		filepath := path.Join(dir, testCase.filename)
		err := WriteToExcel(filepath, testCase.rows)
		assert.NoError(t, err)

		// Check the data written to the file
		actualRead, err := ReadFromExcel(filepath, "Sheet1")
		assert.NoError(t, err)
		assert.Equal(t, testCase.rows, actualRead)
	}

	defer os.RemoveAll(dir)
}
