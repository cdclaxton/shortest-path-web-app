package i2chart

import (
	"fmt"

	"github.com/xuri/excelize/v2"
)

// Column letters used by Excel
const columnLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func columnIndexToLetter(idx int) (string, error) {

	// Precondition
	if idx < 0 || idx >= len(columnLetters) {
		return "", fmt.Errorf("Column index is out-of-bounds")
	}

	return string(columnLetters[idx]), nil
}

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

func WriteToExcel(filepath string, rows [][]string) error {

	// Preconditions
	if len(filepath) == 0 {
		return fmt.Errorf("Filepath is empty")
	}

	if rows == nil {
		return fmt.Errorf("Rows to write is nil")
	}

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

func ReadFromExcel(filepath string, sheetName string) ([][]string, error) {

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
