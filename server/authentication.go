package main

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/xdg-go/scram"
)

func (server *PostgresServer) authenticatePassword() error {
	if server.config.EncryptedPassword == "" {
		return nil
	}
	failure := func() error {
		return NewClientError(PG_ERROR_SEVERITY_FATAL, "28P01", "password authentication failed", errors.New("password authentication failed"))
	}
	credentials, err := scramCredentials(server.config.EncryptedPassword)
	if err != nil {
		return failure()
	}
	verifier, err := scram.SHA256.NewServer(func(_ string) (scram.StoredCredentials, error) { return credentials, nil })
	if err != nil {
		return failure()
	}
	conversation := verifier.NewConversation()
	if err := (*server.conn).SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	defer (*server.conn).SetDeadline(time.Time{})
	if err := server.backend.SetAuthType(pgproto3.AuthTypeSASL); err != nil {
		return err
	}
	server.writeMessages(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
	message, err := server.backend.Receive()
	if err != nil {
		return failure()
	}
	initial, ok := message.(*pgproto3.SASLInitialResponse)
	if !ok || initial.AuthMechanism != "SCRAM-SHA-256" {
		return failure()
	}
	challenge, err := conversation.Step(string(initial.Data))
	if err != nil {
		return failure()
	}
	if err := server.backend.SetAuthType(pgproto3.AuthTypeSASLContinue); err != nil {
		return err
	}
	server.writeMessages(&pgproto3.AuthenticationSASLContinue{Data: []byte(challenge)})
	message, err = server.backend.Receive()
	if err != nil {
		return failure()
	}
	response, ok := message.(*pgproto3.SASLResponse)
	if !ok {
		return failure()
	}
	final, err := conversation.Step(string(response.Data))
	if err != nil || !conversation.Done() || !conversation.Valid() {
		return failure()
	}
	server.writeMessages(&pgproto3.AuthenticationSASLFinal{Data: []byte(final)})
	return nil
}

func scramCredentials(secret string) (scram.StoredCredentials, error) {
	invalid := errors.New("invalid SCRAM verifier")
	parts := strings.Split(secret, "$")
	if len(parts) != 3 || parts[0] != "SCRAM-SHA-256" {
		return scram.StoredCredentials{}, invalid
	}
	factors, keys := strings.Split(parts[1], ":"), strings.Split(parts[2], ":")
	if len(factors) != 2 || len(keys) != 2 {
		return scram.StoredCredentials{}, invalid
	}
	iterations, err := strconv.Atoi(factors[0])
	if err != nil || iterations <= 0 {
		return scram.StoredCredentials{}, invalid
	}
	salt, err := base64.StdEncoding.DecodeString(factors[1])
	if err != nil || len(salt) == 0 {
		return scram.StoredCredentials{}, invalid
	}
	stored, err := base64.StdEncoding.DecodeString(keys[0])
	if err != nil || len(stored) != 32 {
		return scram.StoredCredentials{}, invalid
	}
	key, err := base64.StdEncoding.DecodeString(keys[1])
	if err != nil || len(key) != 32 {
		return scram.StoredCredentials{}, invalid
	}
	return scram.StoredCredentials{KeyFactors: scram.KeyFactors{Salt: string(salt), Iters: iterations}, StoredKey: stored, ServerKey: key}, nil
}
