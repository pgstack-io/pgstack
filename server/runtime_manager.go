package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DUCKDB_RESTART_INTERVAL = 1 * time.Hour
)

type ServerRuntime struct {
	DuckdbClient *DuckdbClient
	QueryHandler *QueryHandler
	RequestMu    *sync.RWMutex
	Expired      bool
	Closed       chan struct{}
}

type RuntimeManager struct {
	mu             sync.Mutex
	runtime        *ServerRuntime
	ready          chan struct{}
	runtimeFactory func(*Config) (*ServerRuntime, error)
}

func (manager *RuntimeManager) Acquire(config *Config) (*QueryHandler, func(), error) {
	for {
		manager.mu.Lock()
		if manager.ready != nil {
			ready := manager.ready
			manager.mu.Unlock()
			<-ready
			continue
		}

		if manager.runtime != nil {
			runtime := manager.runtime
			if runtime.Expired {
				closed := runtime.Closed
				manager.mu.Unlock()
				<-closed
				continue
			}
			runtime.RequestMu.RLock()
			manager.mu.Unlock()
			return runtime.QueryHandler, runtime.RequestMu.RUnlock, nil
		}

		ready := make(chan struct{})
		manager.ready = ready
		manager.mu.Unlock()

		runtime, err := manager.createRuntime(config)

		manager.mu.Lock()
		if err == nil {
			manager.runtime = runtime
		}
		manager.ready = nil
		close(ready)
		manager.mu.Unlock()

		if err != nil {
			return nil, nil, err
		}
		go manager.expireRuntimeAfter(config, runtime)
	}
}

func (manager *RuntimeManager) createRuntime(config *Config) (*ServerRuntime, error) {
	if manager.runtimeFactory != nil {
		return manager.runtimeFactory(config)
	}

	duckdbClient, err := NewDuckdbClient(config, duckdbBootQueris(config))
	if err != nil {
		return nil, err
	}
	queryHandler, err := NewQueryHandler(config, duckdbClient)
	if err != nil {
		duckdbClient.Close()
		return nil, err
	}

	runtime := &ServerRuntime{
		DuckdbClient: duckdbClient,
		QueryHandler: queryHandler,
		RequestMu:    &sync.RWMutex{},
		Closed:       make(chan struct{}),
	}
	LogInfo(config, "DuckDB: Connected for database", config.Database)
	return runtime, nil
}

func (manager *RuntimeManager) expireRuntimeAfter(config *Config, runtime *ServerRuntime) {
	timer := time.NewTimer(DUCKDB_RESTART_INTERVAL)
	defer timer.Stop()
	<-timer.C

	manager.expireRuntime(config, runtime)
}

func (manager *RuntimeManager) expireRuntime(config *Config, runtime *ServerRuntime) {
	manager.mu.Lock()
	if manager.runtime != runtime {
		manager.mu.Unlock()
		return
	}
	runtime.Expired = true
	manager.mu.Unlock()

	runtime.RequestMu.Lock()
	logPodMemoryUsage(config)
	if runtime.DuckdbClient != nil {
		runtime.DuckdbClient.Close()
	}
	runtime.RequestMu.Unlock()

	manager.mu.Lock()
	if manager.runtime == runtime {
		manager.runtime = nil
	}
	close(runtime.Closed)
	manager.mu.Unlock()

	LogInfo(config, "DuckDB: Closed expired database", config.Database)
}

func logPodMemoryUsage(config *Config) {
	memoryUsage := "unavailable"
	data, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err == nil {
		bytes, parseErr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if parseErr == nil {
			memoryUsage = formatBytes(bytes)
		}
	}

	LogInfo(config, fmt.Sprintf("Pod memory before closing DuckDB: %s", memoryUsage))
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	value := float64(bytes)
	for _, suffix := range []string{"KB", "MB", "GB", "TB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, suffix)
		}
	}

	return fmt.Sprintf("%.2f PB", value/unit)
}
