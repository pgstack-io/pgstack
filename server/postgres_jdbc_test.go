package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

// Run with POSTGRES_JDBC_JAR pointing to a PostgreSQL JDBC driver.
func TestPostgresJDBCCursor(t *testing.T) {
	jar := os.Getenv("POSTGRES_JDBC_JAR")
	if jar == "" {
		t.Skip("set POSTGRES_JDBC_JAR to run the real JDBC cursor smoke test")
	}
	java, err := exec.LookPath("java")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PGSTACK_AUTH_MODE", "scram")
	handler := initQueryHandlerForTest(t)
	// Protocol tests use passwordless startup in both cloud and standalone builds.
	handler.Config.EncryptedPassword = ""
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(30 * time.Second))
		NewPostgresServer(handler.Config, &conn).Run(func(*Config) (*QueryHandler, func(), error) { return handler, func() {}, nil })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, java, "-cp", jar, "testdata/CursorRead.java", "jdbc:postgresql://"+listener.Addr().String()+"/pgstack?user=user&sslmode=disable")
	output, err := command.CombinedOutput()
	listener.Close()
	<-done
	if err != nil {
		t.Fatalf("JDBC smoke test: %v\n%s", err, output)
	}
	t.Log(string(output))
}
