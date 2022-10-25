package i2chart

import (
	"fmt"
	"strconv"

	"github.com/cdclaxton/shortest-path-web-app/logging"
	"github.com/xuri/excelize/v2"
)

// Column letters used by Excel
const columnLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// columnIndexToLetter converts a column index (integer) to a letter (as used by Excel).
func columnIndexToLetter(idx int) (string, error) {

	// Precondition
	if idx < 0 || idx >= len(columnLetters) {
		return "", fmt.Errorf("Column index is out-of-bounds")
	}

	return string(columnLetters[idx]), nil
}

// excelCellIndex (as a string) given the column index and row index.
func excelCellIndex(columnIndex int, rowIndex int) (string, error) {

	// Precondition
	if columnIndex < 0 || rowIndex < 0 {
		return "", fmt.Errorf("Invalid cell index (%v,%v)", columnIndex, rowIndex)
	}

	// Letter for the column
	columnLetter, err := columnIndexToLetter(columnIndex)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v%v", columnLetter, rowIndex+1), nil
}

// WriteToExcel writes the rows to the Excel file at filepath.
func WriteToExcel(filepath string, rows [][]string) error {

	// Preconditions
	if len(filepath) == 0 {
		return fmt.Errorf("Filepath is empty")
	}

	if rows == nil {
		return fmt.Errorf("Rows to write is nil")
	}

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", filepath).
		Str("numberOfRows", strconv.Itoa(len(rows))).
		Msg("Writing Excel file")

	// Create a new in-memory Excel file
	f := excelize.NewFile()

	// Walk through each row
	for rowIdx, row := range rows {

		// walk through each column in the row
		for colIdx, value := range row {

			// Cell index
			cellIndex, err := excelCellIndex(colIdx, rowIdx)
			if err != nil {
				return err
			}

			// Write the value to the cell
			f.SetCellValue("Sheet1", cellIndex, value)
		}
	}

	// Save the spreadsheet
	return f.SaveAs(filepath)
}

// ReadFromExcel reads sheet sheetName from file at filepath.
func ReadFromExcel(filepath string, sheetName string) ([][]string, error) {

	logging.Logger.Info().
		Str(logging.ComponentField, componentName).
		Str("filepath", filepath).
		Str("sheetName", sheetName).
		Msg("Reading Excel file")

	// Open the Excel file
	file, err := excelize.OpenFile(filepath)
	if err != nil {
		return nil, err
	}

	// Read all of the rows in the sheet
	excelRows, err := file.GetRows(sheetName)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Return the rows and close the Excel file
	return excelRows, file.Close()
}
