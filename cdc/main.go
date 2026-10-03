package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config := LoadConfig()

	LogInfo(config, "Starting cdc...")
	LogInfo(config, fmt.Sprintf("Slot: %s, Publication: %s", config.SlotName, config.PublicationName))
	if len(config.IncludedTables) > 0 {
		LogInfo(config, fmt.Sprintf("Included tables: %v", config.IncludedTables))
	}
	if len(config.ExcludedTables) > 0 {
		LogInfo(config, fmt.Sprintf("Excluded tables: %v", config.ExcludedTables))
	}
	if len(config.IgnoreChangeColumns) > 0 {
		LogInfo(config, fmt.Sprintf("Ignore change columns: %v", config.IgnoreChangeColumns))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := SetupReplicaIdentity(ctx, config); err != nil {
		LogWarn(config, "Failed to setup replica identity:", err)
	}

	handler, err := NewReplicationHandler(config)
	if err != nil {
		log.Fatalf("Failed to create replication handler: %v", err)
	}
	defer handler.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		LogInfo(config, "Shutting down...")
		cancel()
	}()

	if err := handler.Start(ctx); err != nil {
		if err != context.Canceled {
			log.Fatalf("Replication error: %v", err)
		}
	}

	LogInfo(config, "Stopped")
}
