package main

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
)

func PanicIfError(config *Config, err error) {
	if err != nil {
		printUnexpectedError(config, err)
		os.Exit(1)
	}
}

func Panic(config *Config, message string) {
	err := errors.New(message)
	PanicIfError(config, err)
}

func PrintErrorAndExit(config *Config, message string) {
	LogError(config, message+"\n")
	os.Exit(1)
}

func HandleUnexpectedPanic(config *Config) {
	func() {
		if r := recover(); r != nil {
			err, _ := r.(error)
			printUnexpectedError(config, err)
			os.Exit(1)
		}
	}()
}

func printUnexpectedError(config *Config, err error) {
	errorMessage := err.Error()
	stackTrace := string(debug.Stack())

	fmt.Println("Unexpected error:", errorMessage)
	fmt.Println(stackTrace)
}
