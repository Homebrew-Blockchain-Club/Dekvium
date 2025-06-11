package pbft

import (
	c "Dekvium/common"
	"Dekvium/net"
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
)

// RequestHandler handles client requests and interacts with the PBFT node.
type RequestHandler struct {
	node *Node
}

// NewRequestHandler creates a new RequestHandler bound to a Node.
func NewRequestHandler(node *Node) *RequestHandler {
	return &RequestHandler{node: node}
}

// HandleClientRequest processes a client request, writes it to the journal, and initiates consensus if primary.
func (h *RequestHandler) HandleClientRequest(req c.ClientRequest) c.Message {
	n := h.node
	logrus.Infof("%s: Received client request: op=%s, key=%s", c.CurFuncName(), req.Operation, req.Key)

	// Marshal client request as payload
	payload, err := json.Marshal(req)
	if err != nil {
		logrus.Errorf("%s: Failed to marshal client request: %v", c.CurFuncName(), err)
		return c.Message{}
	}

	// Create a PrePrepare PBFT message for the request
	preprepareMsg := c.Message{
		Type:      c.PrePrepare,
		View:      n.view,
		Sequence:  n.sequence,
		SenderID:  n.Config.ID,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	// Compute digest and sign
	Digest(&preprepareMsg)
	Sign(&preprepareMsg, n.Config.ParsedPrivateKey)

	// Write to journal
	n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey1(preprepareMsg), preprepareMsg}
	logrus.Infof("%s: PrePrepare message journaled with seq=%d, digest=%s", c.CurFuncName(), n.sequence, preprepareMsg.Digest)

	// If this node is the primary, broadcast preprepare to all peers (including itself)
	if n.Config.ID == n.PrimaryNodeNumber() {
		addrs := make([]string, 0)
		for _, peer := range n.Config.Peers {
			addrs = append(addrs, peer.Addr)
		}
		logrus.Infof("%s: Node is primary, broadcasting preprepare to %d peers (including self)", c.CurFuncName(), len(addrs))
		// Broadcast to all peers (including self)
		go net.BroadcastMessage(addrs, &preprepareMsg)
	} else {
		logrus.Infof("%s: Node is not primary, waiting for preprepare from primary", c.CurFuncName())
	}

	// Wait for response from consensusFSM (reply message)
	resp := <-n.respChan
	logrus.Infof("%s: Received response from consensusFSM for seq=%d", c.CurFuncName(), resp.Sequence)
	return resp
}
