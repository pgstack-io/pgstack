package main

import (
	"net/url"
	"os"
	"strings"
)

type Config struct {
	DatabaseURL         string
	LogLevel            string
	SlotName            string
	PublicationName     string
	IncludedTables      []string
	ExcludedTables      []string
	IgnoreChangeColumns []string
	NatsURL             string
	NatsSubject         string
	AuditConfigured     bool
	SearchTables        []string
	SearchSnapshot      bool
}

func LoadConfig() *Config {
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")
	databaseURL = normalizeDatabaseURL(databaseURL)

	config := &Config{
		DatabaseURL:     databaseURL,
		LogLevel:        getEnv("LOG_LEVEL", LOG_LEVEL_INFO),
		SlotName:        getEnv("SLOT_NAME", "pgstack"),
		PublicationName: getEnv("PUBLICATION_NAME", "pgstack"),
		NatsURL:         getEnv("NATS_URL", ""),
		NatsSubject:     getEnv("NATS_SUBJECT", ""),
		SearchSnapshot:  getEnvBool("SEARCH_SNAPSHOT", false),
	}

	if searchTables := os.Getenv("SEARCH_TABLES"); searchTables != "" {
		config.SearchTables = splitAndTrim(searchTables)
	}

	if !isValidLogLevel(config.LogLevel) {
		panic("Invalid log level " + config.LogLevel + ". Must be one of " + strings.Join(LOG_LEVELS, ", "))
	}

	includedTables, includedConfigured := os.LookupEnv("AUDIT_INCLUDED_TABLES")
	excludedTables, excludedConfigured := os.LookupEnv("AUDIT_EXCLUDED_TABLES")
	if includedConfigured && excludedConfigured {
		panic("AUDIT_INCLUDED_TABLES and AUDIT_EXCLUDED_TABLES cannot both be set")
	}
	config.AuditConfigured = includedConfigured || excludedConfigured
	if includedConfigured {
		config.IncludedTables = splitAndTrimNonEmpty(includedTables)
	}
	if excludedConfigured {
		config.ExcludedTables = splitAndTrimNonEmpty(excludedTables)
	}

	if ignoreChangeColumns := os.Getenv("AUDIT_IGNORE_CHANGE_COLUMNS"); ignoreChangeColumns != "" {
		config.IgnoreChangeColumns = splitAndTrimNonEmpty(ignoreChangeColumns)
	}

	return config
}

func splitAndTrim(value string) []string {
	values := strings.Split(value, ",")
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
	return values
}

func splitAndTrimNonEmpty(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return splitAndTrim(value)
}

func getEnvBool(key string, defaultValue bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return defaultValue
	}
	return value == "true" || value == "1" || value == "yes"
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func normalizeDatabaseURL(raw string) string {
	// pgx expects URL userinfo to be escaped; normalize raw passwords with reserved characters.
	if _, err := url.Parse(raw); err == nil {
		return raw
	}

	schemeIdx := strings.Index(raw, "://")
	if schemeIdx == -1 {
		return raw
	}

	scheme := raw[:schemeIdx]
	remainder := raw[schemeIdx+3:]
	authorityEnd := strings.IndexAny(remainder, "/?#")
	authority := remainder
	suffix := ""
	if authorityEnd != -1 {
		authority = remainder[:authorityEnd]
		suffix = remainder[authorityEnd:]
	}

	atIdx := strings.LastIndex(authority, "@")
	if atIdx == -1 {
		return raw
	}

	userInfo := authority[:atIdx]
	host := authority[atIdx+1:]
	if host == "" {
		return raw
	}

	username, password, hasPassword := strings.Cut(userInfo, ":")
	normalized := scheme + "://" + host + suffix
	parsed, err := url.Parse(normalized)
	if err != nil {
		return raw
	}

	if hasPassword {
		parsed.User = url.UserPassword(username, password)
	} else {
		parsed.User = url.User(username)
	}

	return parsed.String()
}

func (c *Config) ShouldProcessTable(schema, table string) bool {
	return c.ShouldAuditTable(schema, table) || c.ShouldSearchTable(schema, table)
}

func (c *Config) ShouldAuditTable(schema, table string) bool {
	if !c.AuditConfigured {
		return false
	}
	fullName := schema + "." + table

	if len(c.ExcludedTables) > 0 {
		for _, excluded := range c.ExcludedTables {
			if excluded == fullName || excluded == table {
				return false
			}
		}
	}

	if len(c.IncludedTables) > 0 {
		for _, included := range c.IncludedTables {
			if included == fullName || included == table {
				return true
			}
		}
		return false
	}

	return true
}

func (c *Config) ShouldSearchTable(schema, table string) bool {
	fullName := schema + "." + table
	for _, configured := range c.SearchTables {
		if configured == fullName {
			return true
		}
	}
	return false
}

func (c *Config) ShouldIgnoreColumn(schema, table, column string) bool {
	if len(c.IgnoreChangeColumns) == 0 {
		return false
	}

	for _, rule := range c.IgnoreChangeColumns {
		parts := strings.Split(rule, ".")
		if len(parts) != 3 {
			continue
		}

		ruleSchema, ruleTable, ruleColumn := parts[0], parts[1], parts[2]

		// Check schema match
		schemaMatch := ruleSchema == "*" || ruleSchema == schema

		// Check table match
		tableMatch := ruleTable == "*" || ruleTable == table

		// Check column match (supports wildcard suffix like *_at)
		var columnMatch bool
		if strings.HasPrefix(ruleColumn, "*") {
			suffix := strings.TrimPrefix(ruleColumn, "*")
			columnMatch = strings.HasSuffix(column, suffix)
		} else {
			columnMatch = ruleColumn == column
		}

		if schemaMatch && tableMatch && columnMatch {
			return true
		}
	}

	return false
}
