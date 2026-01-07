package dialpad

import (
	"encoding/json"

	"github.com/BemiHQ/BemiDB/src/common"
)

type RecordUser struct {
	Id          int64  `json:"id"`
	Type        string `json:"type"`
	Email       string `json:"email"`
	Phone       string `json:"phone"`
	PhoneNumber string `json:"phone_number"`
	Name        string `json:"name"`
	OfficeId    int64  `json:"office_id"`
}

func (record *RecordUser) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = record.Id
	result["type"] = record.Type
	result["name"] = record.Name
	result["phone"] = record.Phone
	result["phone_display"] = record.PhoneNumber
	result["email"] = record.Email
	result["office_id"] = record.OfficeId

	return result
}

type RecordContact struct {
	Id          int64  `json:"id"`
	Type        string `json:"type"`
	Email       string `json:"email"`
	Phone       string `json:"phone"`
	PhoneNumber string `json:"phone_number"`
	Name        string `json:"name"`
}

func (record *RecordContact) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = record.Id
	result["type"] = record.Type
	result["name"] = record.Name
	result["phone"] = record.Phone
	result["phone_display"] = record.PhoneNumber
	result["email"] = record.Email

	return result
}

// ---------------------------------------------------------------------------------------------------------------------

type RecordSmsMessage struct {
	Id             int64         `json:"id"`
	CreatedDate    int64         `json:"created_date"`
	Direction      string        `json:"direction"`
	EventTimestamp int64         `json:"event_timestamp"`
	FromNumber     string        `json:"from_number"`
	ToNumber       []string      `json:"to_number"`
	Text           string        `json:"text"`
	Mms            bool          `json:"mms"`
	MmsUrl         *string       `json:"mms_url"`
	IsInternal     bool          `json:"is_internal"`
	MessageStatus  string        `json:"message_status"`
	Contact        RecordContact `json:"contact"`
	Target         RecordUser    `json:"target"`
	Admins         []RecordUser  `json:"admins"`
}

func (record *RecordSmsMessage) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	result["id"] = record.Id
	result["created_at"] = record.CreatedDate
	result["emitted_at"] = record.EventTimestamp
	result["direction"] = record.Direction
	result["from"] = record.FromNumber
	result["to"] = record.ToNumber
	result["text"] = record.Text
	result["message_status"] = record.MessageStatus
	result["is_internal"] = record.IsInternal
	result["mms"] = record.Mms
	result["mms_url"] = record.MmsUrl
	result["contact"] = ToJson(record.Contact.ToMap())
	result["target"] = ToJson(record.Target.ToMap())
	admins := []map[string]interface{}{}
	for _, admin := range record.Admins {
		admins = append(admins, admin.ToMap())
	}
	result["admins"] = ToJson(admins)

	return result
}

func SmsMessagesIcebergSchemaColumns(config *common.CommonConfig) []*common.IcebergSchemaColumn {
	return []*common.IcebergSchemaColumn{
		{Config: config, ColumnName: "id", ColumnType: common.IcebergColumnTypeLong, Position: 1},
		{Config: config, ColumnName: "created_at", ColumnType: common.IcebergColumnTypeTimestamp, Position: 2, DatetimePrecision: 3},
		{Config: config, ColumnName: "emitted_at", ColumnType: common.IcebergColumnTypeTimestamp, Position: 3, DatetimePrecision: 3},
		{Config: config, ColumnName: "direction", ColumnType: common.IcebergColumnTypeString, Position: 4},
		{Config: config, ColumnName: "from", ColumnType: common.IcebergColumnTypeString, Position: 5},
		{Config: config, ColumnName: "to", ColumnType: common.IcebergColumnTypeString, Position: 6, IsList: true},
		{Config: config, ColumnName: "text", ColumnType: common.IcebergColumnTypeString, Position: 7},
		{Config: config, ColumnName: "message_status", ColumnType: common.IcebergColumnTypeString, Position: 8},
		{Config: config, ColumnName: "is_internal", ColumnType: common.IcebergColumnTypeBoolean, Position: 9},
		{Config: config, ColumnName: "mms", ColumnType: common.IcebergColumnTypeBoolean, Position: 10},
		{Config: config, ColumnName: "mms_url", ColumnType: common.IcebergColumnTypeString, Position: 11},
		{Config: config, ColumnName: "contact", ColumnType: common.IcebergColumnTypeString, Position: 12, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "target", ColumnType: common.IcebergColumnTypeString, Position: 13, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
		{Config: config, ColumnName: "admins", ColumnType: common.IcebergColumnTypeString, Position: 14, LogicalColumnType: common.IcebergLogicalColumnTypeJson},
	}
}

func ToJson(data interface{}) string {
	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return string(jsonData)
}
