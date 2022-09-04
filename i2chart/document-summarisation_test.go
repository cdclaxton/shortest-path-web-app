package i2chart

import (
	"testing"
	"time"

	"github.com/cdclaxton/shortest-path-web-app/graphstore"
	"github.com/stretchr/testify/assert"
)

func TestDocumentTypes(t *testing.T) {
	testCases := []struct {
		docs      []*graphstore.Document
		separator string
		expected  string
	}{
		{
			docs: []*graphstore.Document{
				{DocumentType: "A"},
			},
			separator: ",",
			expected:  "A",
		},
		{
			docs: []*graphstore.Document{
				{DocumentType: "A"},
				{DocumentType: "B"},
			},
			separator: ",",
			expected:  "A,B",
		},
		{
			docs: []*graphstore.Document{
				{DocumentType: "A"},
				{DocumentType: "B"},
				{DocumentType: "A"},
			},
			separator: ",",
			expected:  "A,B",
		},
		{
			docs: []*graphstore.Document{
				{DocumentType: "A"},
				{DocumentType: "B"},
				{DocumentType: "C"},
			},
			separator: ",",
			expected:  "A,B,C",
		},
	}

	for _, testCase := range testCases {
		actual := documentTypes(testCase.docs, testCase.separator)
		assert.Equal(t, testCase.expected, actual)
	}
}

func TestParseDate(t *testing.T) {
	testCases := []struct {
		date        string
		format      string
		expectedUse bool
	}{
		{
			date:        "04/09/2022", // useable date
			format:      "02/01/2006",
			expectedUse: true,
		},
		{
			date:        "04/09/1900", // too far in the past
			format:      "02/01/2006",
			expectedUse: false,
		},
	}

	for _, testCase := range testCases {
		actualTime, use := parseDate(testCase.date, testCase.format)
		assert.Equal(t, testCase.expectedUse, use)

		if testCase.expectedUse {
			assert.Equal(t, testCase.date, actualTime.Format(testCase.format))
		} else {
			assert.Equal(t, time.Time{}, actualTime)
		}
	}

	// Future date
	format := "02/01/2006"
	future := time.Now().AddDate(0, 0, 1).Format(format)
	_, use := parseDate(future, format)
	assert.False(t, use)
}

func TestDateRange(t *testing.T) {
	testCases := []struct {
		dates    []string
		format   string
		expected string
	}{
		{
			dates:    []string{},
			format:   "02/01/2006",
			expected: "",
		},
		{
			// one valid date
			dates:    []string{"04/09/2022"},
			format:   "02/01/2006",
			expected: "04/09/2022",
		},
		{
			// one valid date, one invalid date
			dates:    []string{"04/09/2022", "05 Sept 2022"},
			format:   "02/01/2006",
			expected: "04/09/2022",
		},
		{
			// two valid dates
			dates:    []string{"04/09/2022", "01/03/2021"},
			format:   "02/01/2006",
			expected: "01/03/2021 - 04/09/2022",
		},
		{
			// three valid dates
			dates:    []string{"04/09/2022", "01/03/2021", "05/06/2022"},
			format:   "02/01/2006",
			expected: "01/03/2021 - 04/09/2022",
		},
	}

	for _, testCase := range testCases {
		actual := dateRange(testCase.dates, testCase.format)
		assert.Equal(t, testCase.expected, actual)
	}
}
