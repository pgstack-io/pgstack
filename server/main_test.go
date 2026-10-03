package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeManagerSharesLazyInitialization(t *testing.T) {
	started := make(chan struct{})
	allowInitialization := make(chan struct{})
	manager := &RuntimeManager{
		runtimeFactory: func(config *Config) (*ServerRuntime, error) {
			close(started)
			<-allowInitialization
			return fakeServerRuntime(), nil
		},
	}

	firstDone := make(chan error, 1)
	go func() {
		_, release, err := manager.Acquire(&Config{Database: "slow"})
		if err == nil {
			release()
		}
		firstDone <- err
	}()
	<-started

	secondDone := make(chan error, 1)
	go func() {
		_, release, err := manager.Acquire(&Config{Database: "fast"})
		if err == nil {
			release()
		}
		secondDone <- err
	}()

	select {
	case <-secondDone:
		close(allowInitialization)
		t.Fatal("second initialization should wait for the first lazy startup")
	default:
	}

	close(allowInitialization)
	if err := <-firstDone; err != nil {
		t.Fatalf("first database initialization failed: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second database initialization failed: %v", err)
	}
}

func TestRuntimeManagerWaitsForExpiredRuntimeToCloseBeforeRecreating(t *testing.T) {
	var attempts atomic.Int32
	runtime := fakeServerRuntime()
	manager := &RuntimeManager{
		runtime: runtime,
		runtimeFactory: func(config *Config) (*ServerRuntime, error) {
			attempts.Add(1)
			return fakeServerRuntime(), nil
		},
	}
	config := &Config{Database: "expired"}

	runtime.RequestMu.RLock()
	expired := make(chan struct{})
	go func() {
		manager.expireRuntime(config, runtime)
		close(expired)
	}()

	deadline := time.After(time.Second)
	for {
		manager.mu.Lock()
		isExpired := runtime.Expired
		manager.mu.Unlock()
		if isExpired {
			break
		}

		select {
		case <-deadline:
			runtime.RequestMu.RUnlock()
			t.Fatal("runtime was not marked expired")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	acquireDone := make(chan error, 1)
	go func() {
		_, release, err := manager.Acquire(config)
		if err == nil {
			release()
		}
		acquireDone <- err
	}()

	select {
	case <-acquireDone:
		runtime.RequestMu.RUnlock()
		t.Fatal("new runtime was created before expired runtime closed")
	default:
	}
	if got := attempts.Load(); got != 0 {
		runtime.RequestMu.RUnlock()
		t.Fatalf("runtime creation attempts before close = %d, want 0", got)
	}

	runtime.RequestMu.RUnlock()
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("expired runtime did not close")
	}
	if err := <-acquireDone; err != nil {
		t.Fatalf("runtime acquisition after close failed: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("runtime creation attempts after close = %d, want 1", got)
	}
}

func TestRuntimeManagerRetriesAfterInitializationFailure(t *testing.T) {
	var attempts atomic.Int32
	manager := &RuntimeManager{
		runtimeFactory: func(config *Config) (*ServerRuntime, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("boot failed")
			}
			return fakeServerRuntime(), nil
		},
	}
	config := &Config{Database: "retry"}

	if _, _, err := manager.Acquire(config); err == nil {
		t.Fatal("expected first initialization to fail")
	}
	_, release, err := manager.Acquire(config)
	if err != nil {
		t.Fatalf("expected initialization retry to succeed: %v", err)
	}
	release()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("initialization attempts = %d, want 2", got)
	}
}

func TestRuntimeManagerSharesInitializationWithinDatabase(t *testing.T) {
	var attempts atomic.Int32
	started := make(chan struct{})
	allowInitialization := make(chan struct{})
	manager := &RuntimeManager{
		runtimeFactory: func(config *Config) (*ServerRuntime, error) {
			attempts.Add(1)
			close(started)
			<-allowInitialization
			return fakeServerRuntime(), nil
		},
	}
	config := &Config{Database: "shared"}
	done := make(chan error, 2)

	acquire := func() {
		_, release, err := manager.Acquire(config)
		if err == nil {
			release()
		}
		done <- err
	}
	go acquire()
	<-started
	go acquire()
	close(allowInitialization)

	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("database initialization failed: %v", err)
		}
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("initialization attempts = %d, want 1", got)
	}
}

func fakeServerRuntime() *ServerRuntime {
	return &ServerRuntime{
		QueryHandler: &QueryHandler{},
		RequestMu:    &sync.RWMutex{},
		Closed:       make(chan struct{}),
	}
}
