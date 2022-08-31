package i2chart

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"
)

// findKeywords in a format string.
func findKeywords(format string) ([]string, error) {

	// Compile the regex and in the highly unlikely event that it fails to compile,
	// return the error message.
	r, err := regexp.Compile("<.*?>")
	if err != nil {
		log.Error().Str("Component", "i2ChartBuilder").Msg("Regex failed to compile")
		return nil, err
	}

	// Find all (hence the -1) occurrences in the format string
	matches := r.FindAllString(format, -1)
	if matches == nil {
		return []string{}, nil
	}

	return matches, nil
}

// Substitute the keywords in the format string. If a keyword is missing, use the placeholder.
func Substitute(format string, keywordToValue map[string]string, missing string) (string, error) {

	// Preconditions
	for keyword := range keywordToValue {
		if len(keyword) == 0 {
			return "", fmt.Errorf("Empty keyword found")
		}

		if strings.Contains(keyword, "<") || strings.Contains(keyword, ">") {
			return "", fmt.Errorf("Keyword contains illegal characters")
		}
	}

	// Prepend and append angle brackets to the keyword for substitution
	encapsulatedKeywordToValue := map[string]string{}
	for keyword, value := range keywordToValue {
		encapsulatedKeywordToValue["<"+keyword+">"] = value
	}

	// Find the keywords in the format string
	keywordsInFormat, err := findKeywords(format)
	if err != nil {
		return "", err
	}

	// Walk through each keyword and make the substitution
	for _, keyword := range keywordsInFormat {

		replacement, found := encapsulatedKeywordToValue[keyword]
		if !found {
			format = strings.ReplaceAll(format, keyword, missing)
		} else {
			format = strings.ReplaceAll(format, keyword, replacement)
		}

	}

	return format, nil
}
