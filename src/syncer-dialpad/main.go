package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
	"github.com/BemiHQ/BemiDB/src/syncer-dialpad/lib"
)

func init() {
	dialpad.RegisterFlags()
}

func main() {
	config := dialpad.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	storageS3 := common.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, common.SYNCER_DUCKDB_BOOT_QUERIES)
	syncer := dialpad.NewSyncer(config, storageS3, duckdbClient)
	syncer.Sync()
}
