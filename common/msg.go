package common

import "time"

// MessageType defines PBFT message types.
type MessageType int

const (
	PrePrepare MessageType = iota
	Prepare
	Commit
	Reply
	ViewChange
	NewView
	Request
)

var MessageType2Str = []string{
	"preprepare",
	"prepare",
	"commit",
	"reply",
	"view-change",
	"newview",
	"request",
}
var Str2MessageType = map[string]MessageType{
	"preprepare":  PrePrepare,
	"prepare":     Prepare,
	"commit":      Commit,
	"reply":       Reply,
	"view-change": ViewChange,
	"newview":     NewView,
	"request":     Request,
}

// Message represents a PBFT protocol message.
type Message struct {
	Type      MessageType
	View      int
	Sequence  int
	Digest    string
	SenderID  int
	Payload   []byte
	Timestamp time.Time
	Signature string
}
