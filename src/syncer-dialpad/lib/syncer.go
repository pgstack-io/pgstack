package dialpad

import (
	"context"
	"encoding/json"
	"time"

	js "github.com/nats-io/nats.go/jetstream"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	TABLE_SMS_MESSAGES = "sms_messages"

	NATS_TIMEOUT          = 30 * time.Second
	NATS_FETCH_BATCH_SIZE = 100
)

type Syncer struct {
	Config       *Config
	StorageS3    *common.StorageS3
	DuckdbClient *common.DuckdbClient
}

func NewSyncer(config *Config, storageS3 *common.StorageS3, duckdbClient *common.DuckdbClient) *Syncer {
	return &Syncer{
		Config:       config,
		StorageS3:    storageS3,
		DuckdbClient: duckdbClient,
	}
}

func (syncer *Syncer) Sync() {
	common.SendAnonymousAnalytics(syncer.Config.CommonConfig, "syncer-dialpad-start", syncer.name())

	natsCtx, cancel := context.WithTimeout(context.Background(), NATS_TIMEOUT)
	defer cancel()
	natsConsumer := syncer.natsConsumer(natsCtx)
	natsFetchTimeout := time.Duration(syncer.Config.Nats.FetchTimeoutSeconds) * time.Second

	icebergTable := common.NewIcebergTable(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, common.IcebergSchemaTable{
		Schema: syncer.Config.DestinationSchemaName,
		Table:  TABLE_SMS_MESSAGES,
	})
	icebergSchemaColumns := SmsMessagesIcebergSchemaColumns(syncer.Config.CommonConfig)

	for {
		// Fetch messages from NATS
		common.LogInfo(syncer.Config.CommonConfig, "Fetching messages from NATS with", natsFetchTimeout, "timeout...")
		messagesBatch, err := natsConsumer.Fetch(NATS_FETCH_BATCH_SIZE, js.FetchMaxWait(natsFetchTimeout))
		common.PanicIfError(syncer.Config.CommonConfig, err)
		var messages []js.Msg
		var messagesBytes [][]byte
		for message := range messagesBatch.Messages() {
			messages = append(messages, message)
			messagesBytes = append(messagesBytes, message.Data())
		}
		common.LogInfo(syncer.Config.CommonConfig, "Fetched", len(messagesBytes), "messages from NATS")

		// Write messages to Iceberg
		syncer.WriteToIceberg(messagesBytes, icebergTable, icebergSchemaColumns)

		// Acknowledge messages
		for _, message := range messages {
			err := message.Ack()
			common.PanicIfError(syncer.Config.CommonConfig, err)
		}
	}
}

func (syncer *Syncer) WriteToIceberg(messagesBytes [][]byte, icebergTable *common.IcebergTable, icebergSchemaColumns []*common.IcebergSchemaColumn) {
	cappedBuffer := common.NewCappedBuffer(syncer.Config.CommonConfig, common.DEFAULT_CAPPED_BUFFER_SIZE)
	jsonQueueWriter := common.NewJsonQueueWriter(cappedBuffer)
	for _, messageBytes := range messagesBytes {
		common.LogDebug(syncer.Config.CommonConfig, string(messageBytes))

		var smsMessage RecordSmsMessage
		err := json.Unmarshal(messageBytes, &smsMessage)
		common.PanicIfError(syncer.Config.CommonConfig, err)

		err = jsonQueueWriter.Write(smsMessage.ToMap())
		common.PanicIfError(syncer.Config.CommonConfig, err)
	}
	jsonQueueWriter.Close()

	icebergTableWriter := common.NewIcebergTableWriter(syncer.Config.CommonConfig, syncer.StorageS3, syncer.DuckdbClient, icebergTable, icebergSchemaColumns, 1)
	icebergTableWriter.AppendFromJsonCappedBuffer(common.CursorValue{}, cappedBuffer)
}

func (syncer *Syncer) natsConsumer(natsCtx context.Context) js.Consumer {
	nats := NewNats(syncer.Config)

	natsStream := nats.Stream(natsCtx)
	common.LogInfo(syncer.Config.CommonConfig, "Creating a consumer for subject:", syncer.Config.Nats.Subject)

	natsConsumer, err := natsStream.CreateOrUpdateConsumer(natsCtx, js.ConsumerConfig{
		Durable:       syncer.Config.Nats.ConsumerName,
		FilterSubject: syncer.Config.Nats.Subject,
		MaxAckPending: -1,                   // Instead of default 1000
		AckPolicy:     js.AckExplicitPolicy, // Default
		DeliverPolicy: js.DeliverAllPolicy,  // Default
		AckWait:       30 * time.Second,     // Default (30 seconds)
		MaxDeliver:    -1,                   // Default (re-deliver forever until acked)
	})
	common.PanicIfError(syncer.Config.CommonConfig, err)

	return natsConsumer
}

func (syncer *Syncer) name() string {
	return "dialpad"
}
