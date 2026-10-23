package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_PROTOCOL_QUEUE_SIZE = 32
	PG_CURSOR_IDLE_TIMEOUT = 5 * time.Minute
)

// Receive independently from query execution so EOF can cancel DuckDB work.
// The bounded queue permits normal extended-protocol pipelining.
func (server *PostgresServer) receiveMessages(ctx context.Context, cancel context.CancelFunc) (<-chan pgproto3.FrontendMessage, <-chan struct{}) {
	messages := make(chan pgproto3.FrontendMessage, PG_PROTOCOL_QUEUE_SIZE)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(messages)
		defer func() { cancel(); (*server.conn).Close() }()
		for {
			message, err := server.backend.Receive()
			if err != nil {
				return
			}
			if _, ok := message.(*pgproto3.Terminate); ok {
				return
			}
			// pgproto3 reuses its receive structs and byte buffers. Queue owned copies.
			owned, err := copyFrontendMessage(message)
			if err != nil {
				return
			}
			select {
			case messages <- owned:
			case <-ctx.Done():
				return
			}
		}
	}()
	return messages, done
}

func copyFrontendMessage(message pgproto3.FrontendMessage) (pgproto3.FrontendMessage, error) {
	var copy pgproto3.FrontendMessage
	switch message.(type) {
	case *pgproto3.Parse:
		copy = &pgproto3.Parse{}
	case *pgproto3.Bind:
		copy = &pgproto3.Bind{}
	case *pgproto3.Describe:
		copy = &pgproto3.Describe{}
	case *pgproto3.Execute:
		copy = &pgproto3.Execute{}
	case *pgproto3.Close:
		copy = &pgproto3.Close{}
	case *pgproto3.Query:
		copy = &pgproto3.Query{}
	case *pgproto3.Sync:
		copy = &pgproto3.Sync{}
	case *pgproto3.Flush:
		copy = &pgproto3.Flush{}
	default:
		return nil, fmt.Errorf("unsupported frontend message %T", message)
	}
	encoded, err := message.Encode(nil)
	if err != nil {
		return nil, err
	}
	if err := copy.Decode(encoded[5:]); err != nil {
		return nil, err
	}
	return copy, nil
}
