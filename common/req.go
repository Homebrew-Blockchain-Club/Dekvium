package common

// ClientRequest represents a client request to set or get a value for a specific key.
type ClientRequest struct {
	Operation string // "set" or "get"
	Key       string
	Value     string // Used only for "set" operation; ignored for "get"
}
