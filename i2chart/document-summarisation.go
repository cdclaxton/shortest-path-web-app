// Document summarisation functions.
//
// The i2 chart configuration JSON file permits the construction of different document summary
// information to be present on a link between two entities.
//
// Each function that generates a particular summarisation, such as the range of dates, should be
// resilient to missing attributes of a document.

package i2chart

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/cdclaxton/shortest-path-web-app/set"
)

// Keywords
const (
	numDocsKeyword      = "NUM-DOCS"
	docTypesKeyword     = "DOCUMENT-TYPES"
	docDateRangeKeyword = "DOCUMENT-DATE-RANGE"
)

// Maximum document age for it to be retained
const maxDocumentAgeInYears = 100

// documentTypes of the slice of documents, joined using the separator.
func documentTypes(docs []*graphstore.Document, separator string) string {

	if len(docs) == 0 {
		return ""
	}

	// Set of document types
	types := set.NewSet[string]()
	for _, doc := range docs {
		types.Add(doc.DocumentType)
	}

	// Sorted list of the unique document types
	typesSlice := types.ToSlice()
	sort.Strings(typesSlice)

	return strings.Join(typesSlice, separator)
}

// parseDate and exclude those too far in the past or in the future.
func parseDate(date string, format string) (time.Time, bool) {

	// Try to parse the date from its string representation
	parsed, err := time.Parse(format, date)
	if err != nil {
		return time.Time{}, false
	}

	// If the date is too far in the fast, then ignore it
	oneHundredYearsAgo := time.Now().AddDate(-maxDocumentAgeInYears, 0, 0)
	if parsed.Before(oneHundredYearsAgo) {
		return time.Time{}, false
	}

	// If the date is in the future, then ignore it
	if parsed.After(time.Now()) {
		return time.Time{}, false
	}

	return parsed, true
}

// dateRange in the form (min - max).
func dateRange(dates []string, format string) string {

	// Parse each of the dates
	parsedDates := []time.Time{}
	for _, date := range dates {
		parsed, use := parseDate(date, format)

		if use {
			parsedDates = append(parsedDates, parsed)
		}
	}

	if len(parsedDates) == 0 {
		return ""
	} else if len(parsedDates) == 1 {
		return parsedDates[0].Format(format)
	}

	// Sort the dates
	sort.Slice(parsedDates, func(i, j int) bool {
		return parsedDates[i].Before(parsedDates[j])
	})

	// Earliest and latest dates
	earliest := parsedDates[0].Format(format)
	latest := parsedDates[len(parsedDates)-1].Format(format)

	// Return a string of the date range
	return fmt.Sprintf("%v - %v", earliest, latest)
}

// documentDates as a range if there is a date attribute and a date format.
func documentDates(docs []*graphstore.Document, dateAttribute string,
	dateFormat string) string {

	if len(docs) == 0 {
		return ""
	}

	if len(dateAttribute) == 0 || len(dateFormat) == 0 {
		return ""
	}

	// Extract the dates of the documents
	dates := []string{}
	for _, doc := range docs {
		value, found := doc.Attributes[dateAttribute]
		if !found {
			continue
		}

		dates = append(dates, value)
	}

	// Return the date range
	return dateRange(dates, dateFormat)
}

// keywordsForDocs summarises the key properties of a list of documents.
func keywordsForDocs(docs []*graphstore.Document, dateAttribute string,
	dateFormat string) map[string]string {

	return map[string]string{
		numDocsKeyword:      fmt.Sprintf("%d", len(docs)),
		docTypesKeyword:     documentTypes(docs, ", "),
		docDateRangeKeyword: documentDates(docs, dateAttribute, dateFormat),
	}
}
