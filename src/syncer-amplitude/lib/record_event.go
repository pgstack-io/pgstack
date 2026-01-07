package amplitude

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	EVENTS_TABLE_NAME = "events"
)

// https://amplitude.com/docs/apis/analytics/export
type RecordEvent struct {
	Adid                    string                 `json:"adid"`
	AmplitudeAttributionIDs string                 `json:"amplitude_attribution_ids"`
	AmplitudeEventType      string                 `json:"amplitude_event_type"`
	AmplitudeID             int64                  `json:"amplitude_id"`
	App                     int                    `json:"app"`
	City                    string                 `json:"city"`
	ClientEventTime         string                 `json:"client_event_time"`
	ClientUploadTime        string                 `json:"client_upload_time"`
	Country                 string                 `json:"country"`
	Data                    map[string]interface{} `json:"data"`
	DataType                string                 `json:"data_type"`
	DeviceBrand             string                 `json:"device_brand"`
	DeviceCarrier           string                 `json:"device_carrier"`
	DeviceFamily            string                 `json:"device_family"`
	DeviceID                string                 `json:"device_id"`
	DeviceManufacturer      string                 `json:"device_manufacturer"`
	DeviceModel             string                 `json:"device_model"`
	DeviceType              string                 `json:"device_type"`
	DMA                     string                 `json:"dma"`
	EventID                 int                    `json:"event_id"`
	EventProperties         map[string]interface{} `json:"event_properties"`
	EventTime               string                 `json:"event_time"`
	EventType               string                 `json:"event_type"`
	GlobalUserProperties    map[string]interface{} `json:"global_user_properties"`
	GroupProperties         map[string]interface{} `json:"group_properties"`
	Groups                  map[string]interface{} `json:"groups"`
	IDFA                    string                 `json:"idfa"`
	InsertID                string                 `json:"$insert_id"`
	InsertKey               string                 `json:"$insert_key"`
	IPAddress               string                 `json:"ip_address"`
	IsAttributionEvent      bool                   `json:"is_attribution_event"`
	Language                string                 `json:"language"`
	Library                 string                 `json:"library"`
	LocationLat             float64                `json:"location_lat"`
	LocationLng             float64                `json:"location_lng"`
	OSName                  string                 `json:"os_name"`
	OSVersion               string                 `json:"os_version"`
	PartnerID               string                 `json:"partner_id"`
	Paying                  string                 `json:"paying"`
	Plan                    map[string]interface{} `json:"plan"`
	Platform                string                 `json:"platform"`
	ProcessedTime           string                 `json:"processed_time"`
	Region                  string                 `json:"region"`
	SampleRate              float64                `json:"sample_rate"`
	Schema                  string                 `json:"$schema"`
	ServerReceivedTime      string                 `json:"server_received_time"`
	ServerUploadTime        string                 `json:"server_upload_time"`
	SessionID               int64                  `json:"session_id"`
	SourceID                string                 `json:"source_id"`
	StartVersion            string                 `json:"start_version"`
	UserCreationTime        string                 `json:"user_creation_time"`
	UserID                  string                 `json:"user_id"`
	UserProperties          map[string]interface{} `json:"user_properties"`
	UUID                    string                 `json:"uuid"`
	VersionName             string                 `json:"version_name"`
}

// Normalize $insert_id -> insert_id, $insert_key -> insert_key, $schema -> schema
func (event *RecordEvent) ToMap() map[string]interface{} {
	result := make(map[string]interface{})
	result["adid"] = event.Adid
	result["amplitude_attribution_ids"] = event.AmplitudeAttributionIDs
	result["amplitude_event_type"] = event.AmplitudeEventType
	result["amplitude_id"] = event.AmplitudeID
	result["app"] = event.App
	result["city"] = event.City
	result["client_event_time"] = event.ClientEventTime
	result["client_upload_time"] = event.ClientUploadTime
	result["country"] = event.Country
	result["data"] = event.Data
	result["data_type"] = event.DataType
	result["device_brand"] = event.DeviceBrand
	result["device_carrier"] = event.DeviceCarrier
	result["device_family"] = event.DeviceFamily
	result["device_id"] = event.DeviceID
	result["device_manufacturer"] = event.DeviceManufacturer
	result["device_model"] = event.DeviceModel
	result["device_type"] = event.DeviceType
	result["dma"] = event.DMA
	result["event_id"] = event.EventID
	result["event_properties"] = event.EventProperties
	result["event_time"] = event.EventTime
	result["event_type"] = event.EventType
	result["global_user_properties"] = event.GlobalUserProperties
	result["group_properties"] = event.GroupProperties
	result["groups"] = event.Groups
	result["idfa"] = event.IDFA
	result["insert_id"] = event.InsertID
	result["insert_key"] = event.InsertKey
	result["ip_address"] = event.IPAddress
	result["is_attribution_event"] = event.IsAttributionEvent
	result["language"] = event.Language
	result["library"] = event.Library
	result["location_lat"] = event.LocationLat
	result["location_lng"] = event.LocationLng
	result["os_name"] = event.OSName
	result["os_version"] = event.OSVersion
	result["partner_id"] = event.PartnerID
	result["paying"] = event.Paying
	result["plan"] = event.Plan
	result["platform"] = event.Platform
	result["processed_time"] = event.ProcessedTime
	result["region"] = event.Region
	result["sample_rate"] = event.SampleRate
	result["schema"] = event.Schema
	result["server_received_time"] = event.ServerReceivedTime
	result["server_upload_time"] = event.ServerUploadTime
	result["session_id"] = event.SessionID
	result["source_id"] = event.SourceID
	result["start_version"] = event.StartVersion
	result["user_creation_time"] = event.UserCreationTime
	result["user_id"] = event.UserID
	result["user_properties"] = event.UserProperties
	result["uuid"] = event.UUID
	result["version_name"] = event.VersionName
	return result
}

func EventsIcebergSchemaColumns(config *common.CommonConfig) []*common.IcebergSchemaColumn {
	return []*common.IcebergSchemaColumn{
		{Config: config, ColumnName: "adid", ColumnType: common.IcebergColumnTypeString, Position: 1},
		{Config: config, ColumnName: "amplitude_attribution_ids", ColumnType: common.IcebergColumnTypeString, Position: 2},
		{Config: config, ColumnName: "amplitude_event_type", ColumnType: common.IcebergColumnTypeString, Position: 3},
		{Config: config, ColumnName: "amplitude_id", ColumnType: common.IcebergColumnTypeLong, Position: 4},
		{Config: config, ColumnName: "app", ColumnType: common.IcebergColumnTypeInteger, Position: 5},
		{Config: config, ColumnName: "city", ColumnType: common.IcebergColumnTypeString, Position: 6},
		{Config: config, ColumnName: "client_event_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 7},
		{Config: config, ColumnName: "client_upload_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 8},
		{Config: config, ColumnName: "country", ColumnType: common.IcebergColumnTypeString, Position: 9},
		{Config: config, ColumnName: "data", ColumnType: common.IcebergColumnTypeString, Position: 10, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "data_type", ColumnType: common.IcebergColumnTypeString, Position: 11},
		{Config: config, ColumnName: "device_brand", ColumnType: common.IcebergColumnTypeString, Position: 12},
		{Config: config, ColumnName: "device_carrier", ColumnType: common.IcebergColumnTypeString, Position: 13},
		{Config: config, ColumnName: "device_family", ColumnType: common.IcebergColumnTypeString, Position: 14},
		{Config: config, ColumnName: "device_id", ColumnType: common.IcebergColumnTypeString, Position: 15},
		{Config: config, ColumnName: "device_manufacturer", ColumnType: common.IcebergColumnTypeString, Position: 16},
		{Config: config, ColumnName: "device_model", ColumnType: common.IcebergColumnTypeString, Position: 17},
		{Config: config, ColumnName: "device_type", ColumnType: common.IcebergColumnTypeString, Position: 18},
		{Config: config, ColumnName: "dma", ColumnType: common.IcebergColumnTypeString, Position: 19},
		{Config: config, ColumnName: "event_id", ColumnType: common.IcebergColumnTypeInteger, Position: 20},
		{Config: config, ColumnName: "event_properties", ColumnType: common.IcebergColumnTypeString, Position: 21, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "event_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 22},
		{Config: config, ColumnName: "event_type", ColumnType: common.IcebergColumnTypeString, Position: 23},
		{Config: config, ColumnName: "global_user_properties", ColumnType: common.IcebergColumnTypeString, Position: 24, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "group_properties", ColumnType: common.IcebergColumnTypeString, Position: 25, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "groups", ColumnType: common.IcebergColumnTypeString, Position: 26, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "idfa", ColumnType: common.IcebergColumnTypeString, Position: 27},
		{Config: config, ColumnName: "insert_id", ColumnType: common.IcebergColumnTypeString, Position: 28},
		{Config: config, ColumnName: "insert_key", ColumnType: common.IcebergColumnTypeString, Position: 29},
		{Config: config, ColumnName: "ip_address", ColumnType: common.IcebergColumnTypeString, Position: 30},
		{Config: config, ColumnName: "is_attribution_event", ColumnType: common.IcebergColumnTypeBoolean, Position: 31},
		{Config: config, ColumnName: "language", ColumnType: common.IcebergColumnTypeString, Position: 32},
		{Config: config, ColumnName: "library", ColumnType: common.IcebergColumnTypeString, Position: 33},
		{Config: config, ColumnName: "location_lat", ColumnType: common.IcebergColumnTypeFloat, Position: 34},
		{Config: config, ColumnName: "location_lng", ColumnType: common.IcebergColumnTypeFloat, Position: 35},
		{Config: config, ColumnName: "os_name", ColumnType: common.IcebergColumnTypeString, Position: 36},
		{Config: config, ColumnName: "os_version", ColumnType: common.IcebergColumnTypeString, Position: 37},
		{Config: config, ColumnName: "partner_id", ColumnType: common.IcebergColumnTypeString, Position: 38},
		{Config: config, ColumnName: "paying", ColumnType: common.IcebergColumnTypeBoolean, Position: 39},
		{Config: config, ColumnName: "plan", ColumnType: common.IcebergColumnTypeString, Position: 40, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "platform", ColumnType: common.IcebergColumnTypeString, Position: 41},
		{Config: config, ColumnName: "processed_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 42},
		{Config: config, ColumnName: "region", ColumnType: common.IcebergColumnTypeString, Position: 43},
		{Config: config, ColumnName: "sample_rate", ColumnType: common.IcebergColumnTypeFloat, Position: 44},
		{Config: config, ColumnName: "schema", ColumnType: common.IcebergColumnTypeString, Position: 45},
		{Config: config, ColumnName: "server_received_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 46},
		{Config: config, ColumnName: "server_upload_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 47},
		{Config: config, ColumnName: "session_id", ColumnType: common.IcebergColumnTypeLong, Position: 48},
		{Config: config, ColumnName: "source_id", ColumnType: common.IcebergColumnTypeString, Position: 49},
		{Config: config, ColumnName: "start_version", ColumnType: common.IcebergColumnTypeString, Position: 50},
		{Config: config, ColumnName: "user_creation_time", ColumnType: common.IcebergColumnTypeTimestamp, Position: 51},
		{Config: config, ColumnName: "user_id", ColumnType: common.IcebergColumnTypeString, Position: 52},
		{Config: config, ColumnName: "user_properties", ColumnType: common.IcebergColumnTypeString, Position: 53, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "uuid", ColumnType: common.IcebergColumnTypeString, Position: 54},
		{Config: config, ColumnName: "version_name", ColumnType: common.IcebergColumnTypeString, Position: 55},
	}
}
