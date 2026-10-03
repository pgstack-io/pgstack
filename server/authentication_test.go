package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestStandaloneAuthentication(t *testing.T) {
	configureStandaloneTest(t)
	for _, test := range []struct {
		name, password string
		accepted       bool
	}{
		{"correct", "correct-password", true},
		{"wrong", "wrong-password", false},
		{"empty", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				config := &Config{Database: "pgstack", User: "reader", EncryptedPassword: StringToScramSha256("correct-password")}
				server := NewPostgresServer(config, &conn)
				done <- server.handleStartup()
			}()
			config, err := pgconn.ParseConfig("postgres://reader@" + listener.Addr().String() + "/pgstack?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			config.Password = test.password
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			if conn != nil {
				conn.Close(ctx)
			}
			if (err == nil) != test.accepted {
				t.Fatalf("accepted=%v, error=%v", test.accepted, err)
			}
			serverErr := <-done
			if (serverErr == nil) != test.accepted {
				t.Fatalf("server error=%v", serverErr)
			}
		})
	}
}

func TestStandaloneNoPasswordPacketCannotBypassChallenge(t *testing.T) {
	configureStandaloneTest(t)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		server := NewPostgresServer(&Config{Database: "pgstack", EncryptedPassword: StringToScramSha256("secret")}, &serverConn)
		done <- server.handleStartup()
	}()
	frontend := pgproto3.NewFrontend(clientConn, clientConn)
	frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: 196608, Parameters: map[string]string{"database": "pgstack", "user": "reader"}})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	message, err := frontend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := message.(*pgproto3.AuthenticationSASL); !ok {
		t.Fatalf("expected password challenge, got %T", message)
	}
	frontend.Send(&pgproto3.Query{String: "SELECT 1"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	message, err = frontend.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := message.(*pgproto3.ErrorResponse); !ok || response.Code != "28P01" {
		t.Fatalf("expected authentication failure, got %#v", message)
	}
	if err := <-done; err == nil {
		t.Fatal("startup bypassed authentication")
	}
}

func TestStandaloneHasNoSpecialUser(t *testing.T) {
	configureStandaloneTest(t)
	if startupUserAllowed(&Config{User: "reader"}, "pgstack") {
		t.Fatal("unexpected privileged user")
	}
}
