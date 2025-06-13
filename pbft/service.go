package pbft

import (
	c "Dekvium/common"
	"encoding/json"
)

func (n *Node) service(msg *c.Message) {
	key := GeneratePrevQueryKey1(*msg, "request")
	replyChan := make(chan []journalEntry)
	n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: key}, replyChan}
	reply := <-replyChan
	payload := reply[0].msg.Payload
	var req c.ClientRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		// If payload is invalid, do nothing or log error
		return
	}
	switch req.Operation {
	case "set":
		n.Storage.Set(req.Key, req.Value)
		msg.Payload = []byte("done")
	case "get":
		msg.Payload = []byte(n.Storage.Get(req.Key))
	}
}
