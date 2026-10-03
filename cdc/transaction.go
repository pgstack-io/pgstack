package main

import (
	"time"

	"github.com/jackc/pglogrepl"
)

type Change struct {
	Operation string
	Schema    string
	Table     string
	Position  uint64
	Before    map[string]interface{}
	After     map[string]interface{}
}

type LogicalMessage struct {
	Prefix        string
	Content       []byte
	Transactional bool
	Position      uint64
}

type Transaction struct {
	XID             uint32
	CommitTime      time.Time
	Changes         []Change
	LogicalMessages []LogicalMessage
}

type TransactionBuffer struct {
	currentTxn *Transaction
}

func NewTransactionBuffer() *TransactionBuffer {
	return &TransactionBuffer{}
}

func (tb *TransactionBuffer) Begin(msg *pglogrepl.BeginMessage) {
	tb.currentTxn = &Transaction{
		XID:             msg.Xid,
		Changes:         []Change{},
		LogicalMessages: []LogicalMessage{},
	}
}

func (tb *TransactionBuffer) AddChange(change Change) {
	if tb.currentTxn != nil {
		tb.currentTxn.Changes = append(tb.currentTxn.Changes, change)
	}
}

func (tb *TransactionBuffer) AddLogicalMessage(msg LogicalMessage) {
	if tb.currentTxn != nil {
		tb.currentTxn.LogicalMessages = append(tb.currentTxn.LogicalMessages, msg)
	}
}

func (tb *TransactionBuffer) Commit(msg *pglogrepl.CommitMessage) {
	if tb.currentTxn != nil {
		tb.currentTxn.CommitTime = msg.CommitTime
	}
}

func (tb *TransactionBuffer) GetTransaction() *Transaction {
	txn := tb.currentTxn
	tb.currentTxn = nil
	return txn
}
