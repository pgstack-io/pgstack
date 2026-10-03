package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/crypto/pbkdf2"
)

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func IsLocalHost(host string) bool {
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	return strings.HasPrefix(host, "127.0.0.1") || strings.HasPrefix(host, "localhost")
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func StringContainsUpper(str string) bool {
	for _, char := range str {
		if unicode.IsUpper(char) {
			return true
		}
	}
	return false
}

func StringToScramSha256(password string) string {
	saltLength := 16
	digestLength := 32
	iterations := 4096
	clientKey := []byte("Client Key")
	serverKey := []byte("Server Key")

	salt := make([]byte, saltLength)
	_, err := rand.Read(salt)
	if err != nil {
		return ""
	}

	digestKey := pbkdf2.Key([]byte(password), salt, iterations, digestLength, sha256.New)
	clientKeyHash := hmacSha256Hash(digestKey, clientKey)
	serverKeyHash := hmacSha256Hash(digestKey, serverKey)
	storedKeyHash := sha256Hash(clientKeyHash)

	return fmt.Sprintf(
		"SCRAM-SHA-256$%d:%s$%s:%s",
		iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKeyHash),
		base64.StdEncoding.EncodeToString(serverKeyHash),
	)
}

func hmacSha256Hash(key []byte, message []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(message)
	return hash.Sum(nil)
}

func sha256Hash(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}
