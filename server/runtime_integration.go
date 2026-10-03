package main

func startupUserAllowed(config *Config, user string) bool {
	return config.User == "" || user == config.User
}

func passwordCatalogQuery(_ *Config) string {
	return "CREATE VIEW pg_shadow AS SELECT NULL::text AS usename, NULL::oid AS usesysid, FALSE AS usecreatedb, FALSE AS usesuper, FALSE AS userepl, FALSE AS usebypassrls, NULL::text AS passwd, NULL::timestamp AS valuntil, NULL::text[] AS useconfig WHERE FALSE"
}

func (server *PostgresServer) authenticateStartup(_ string) error {
	return server.authenticatePassword()
}

func validateRuntimeConfig(_ *Config) {}
