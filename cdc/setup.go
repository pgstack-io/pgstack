package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func SetupReplicaIdentity(ctx context.Context, config *Config) error {
	connConfig, err := pgx.ParseConfig(config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to parse database config: %w", err)
	}

	hadNotices := false
	connConfig.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		hadNotices = true
		LogWarn(config, "PostgreSQL notice:", notice.Message)
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	sql := fmt.Sprintf(`
DO $do$
BEGIN
  CREATE OR REPLACE PROCEDURE public._%s_set_replica_identity()
  AS $$
    DECLARE current_schemaname TEXT;
    DECLARE current_tablename TEXT;
  BEGIN
    FOR current_schemaname, current_tablename IN
      SELECT t.schemaname, t.tablename
      FROM pg_tables t
      JOIN pg_class c ON c.relname = t.tablename
      JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
      WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
        AND t.schemaname NOT LIKE 'pg_toast%%'
        AND t.schemaname NOT LIKE 'pg_temp_%%'
        AND c.relkind != 'f' AND c.relreplident != 'f'
    LOOP
      BEGIN
        EXECUTE format('ALTER TABLE %%I.%%I REPLICA IDENTITY FULL', current_schemaname, current_tablename);
      EXCEPTION WHEN insufficient_privilege THEN
        RAISE NOTICE 'Insufficient privilege, skipping table %%.%%', current_schemaname, current_tablename;
      END;
    END LOOP;
  END
  $$ LANGUAGE plpgsql
  SET search_path = '';

  CALL public._%s_set_replica_identity();
EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction THEN
  RAISE NOTICE 'Insufficient privilege or read-only transaction, skipping replica identity setup';
END
$do$ LANGUAGE plpgsql;

DO $do$
BEGIN
  CREATE OR REPLACE FUNCTION public._%s_set_replica_identity_func() RETURNS event_trigger
  AS $$
  BEGIN
    CALL public._%s_set_replica_identity();
  END
  $$ LANGUAGE plpgsql
  SET search_path = '';
EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction THEN
  RAISE NOTICE 'Insufficient privilege or read-only transaction, skipping function creation';
END
$do$ LANGUAGE plpgsql;

DO $do$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'public' AND p.proname = '_%s_set_replica_identity_func') THEN
    DROP EVENT TRIGGER IF EXISTS _%s_set_replica_identity_trigger;
    CREATE EVENT TRIGGER _%s_set_replica_identity_trigger ON ddl_command_end WHEN TAG IN ('CREATE TABLE')
      EXECUTE FUNCTION public._%s_set_replica_identity_func();
  ELSE
    RAISE NOTICE 'Function public._%s_set_replica_identity_func() does not exist, skipping event trigger creation';
  END IF;
EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction OR duplicate_object THEN
  RAISE NOTICE 'Insufficient privilege, read-only transaction, or duplicate object, skipping event trigger creation';
END
$do$ LANGUAGE plpgsql;
`, config.PublicationName, config.PublicationName, config.PublicationName, config.PublicationName,
		config.PublicationName, config.PublicationName, config.PublicationName, config.PublicationName, config.PublicationName)

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to setup replica identity: %w", err)
	}

	if hadNotices {
		LogWarn(config, "Replica identity setup completed with warnings")
		return nil
	}

	LogInfo(config, "Replica identity setup completed")
	return nil
}
