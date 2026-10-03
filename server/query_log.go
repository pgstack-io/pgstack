package main

import (
	"fmt"
	"regexp"
)

var VECTOR_DISTANCE_LITERAL_LOG_PATTERN = regexp.MustCompile(`(?is)(<=>|<->|<#>|<\+>|<~>|<%>)\s*'[^']*'`)
var VECTOR_ARRAY_LITERAL_LOG_PATTERN = regexp.MustCompile(`(?is)\bARRAY\s*\[[^]]*\]\s*::\s*(REAL|FLOAT4?)\s*\[(\d+)\]`)

func redactVectorLiterals(query string) string {
	query = VECTOR_DISTANCE_LITERAL_LOG_PATTERN.ReplaceAllString(query, `$1 '[vector omitted]'`)
	return VECTOR_ARRAY_LITERAL_LOG_PATTERN.ReplaceAllString(query, `ARRAY[vector omitted]::$1[$2]`)
}

func redactLogValues(values []interface{}) []interface{} {
	redacted := append([]interface{}(nil), values...)
	for index, value := range redacted {
		switch value := value.(type) {
		case string:
			redacted[index] = redactVectorLiterals(value)
		case error:
			redacted[index] = redactVectorLiterals(value.Error())
		}
	}
	return redacted
}

func redactVectorVariables(variables []interface{}, specs map[int]SearchParameterSpec) []interface{} {
	redacted := append([]interface{}(nil), variables...)
	for parameterNumber, spec := range specs {
		index := parameterNumber - 1
		if index >= 0 && index < len(redacted) {
			if spec.TextOnly {
				redacted[index] = "[search text omitted]"
			} else if spec.RowCountTable == "" {
				redacted[index] = fmt.Sprintf("[vector omitted: %d dimensions]", spec.Dimensions)
			}
		}
	}
	return redacted
}
