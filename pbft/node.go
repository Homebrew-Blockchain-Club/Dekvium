package pbft

// TODO: migrate request handler to net
// 		 consensus engine should trigger service
import (
	c "Dekvium/common"
	"Dekvium/config"
	"Dekvium/storage"
	"crypto/ecdsa"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// Node is an actor-style FSM node.
type Node struct {
	Config         config.Config
	view           int
	sequence       int
	msgCh          chan c.Message
	stopCh         chan struct{}
	respChan       chan c.Message
	Storage        *storage.Storage
	privkey        *ecdsa.PrivateKey
	consensusFSM   *consensusFSM
	viewchangeFSM  *viewchangeFSM
	checkpointFSM  *checkpointFSM
	journaler      *journaler
	requestHandler *RequestHandler // <-- Add this line
}

// NewNode creates a new PBFT node.
func NewNode(conf config.Config) *Node {
	n := &Node{
		Config:   conf,
		view:     0,
		sequence: 0,
		msgCh:    make(chan c.Message),
		stopCh:   make(chan struct{}),
		respChan: make(chan c.Message),
		Storage:  storage.NewStorage(),
	}
	n.consensusFSM = newConsensusFSM(n)
	n.journaler = NewJournaler(n)
	n.checkpointFSM = newCheckpointFSM(n)
	n.viewchangeFSM = newViewChangeFSM(n)
	n.requestHandler = NewRequestHandler(n) // <-- Initialize the request handler
	n.Config.ParsedPrivateKey = ParsePrivateKey(n.Config.PrivateKey)
	for i := range n.Config.Peers {
		n.Config.Peers[i].ParsedPubKey = ParsePublicKey(n.Config.Peers[i].PubKey)
	}
	go func() {
		for {
			select {
			case msg := <-n.msgCh:
				n.handleMessage(msg)
			case <-n.stopCh:
				return
			}
		}
	}()
	return n
}

// handleMessage is the FSM event handler.
func (n *Node) handleMessage(msg c.Message) {
	switch msg.Type {
	case c.PrePrepare, c.Prepare, c.Commit, c.Reply:
		logrus.Infof("%s: Routing message type=%s to consensusFSM", c.CurFuncName(), c.MessageType2Str[msg.Type])
		n.consensusFSM.mq.TX() <- msg
	case c.ViewChange, c.NewView:
		logrus.Infof("%s: Routing message type=%s to viewchangeFSM", c.CurFuncName(), c.MessageType2Str[msg.Type])
		n.viewchangeFSM.mq.TX() <- msg
	case c.Checkpoint:
		logrus.Infof("%s: Routing message type=%s to checkpointFSM", c.CurFuncName(), c.MessageType2Str[msg.Type])
		n.checkpointFSM.mq.TX() <- msg
	case c.Request:
		logrus.Infof("%s: Routing message type=request to requestHandler (async)", c.CurFuncName())
		go func(msg c.Message) {
			var req c.ClientRequest
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				logrus.Errorf("%s: Failed to unmarshal client request payload: %v", c.CurFuncName(), err)
				return
			}
			n.requestHandler.HandleClientRequest(req)
		}(msg)
	default:
		logrus.Warnf("%s: Unknown message type=%d, ignoring", c.CurFuncName(), msg.Type)
	}
}

// SendMessage sends a message to this node (actor-style).
func (n *Node) SendMessage(msg c.Message) {
	n.msgCh <- msg
}

func (n *Node) PrimaryNodeNumber() int {
	return n.view % len(n.Config.Peers)
}

// PBFTConsensusPassed returns true if the given count satisfies PBFT consensus (>= 2f+1).
func (n *Node) PBFTConsensusPassed(count int) bool {
	f := (len(n.Config.Peers) - 1) / 3
	return count >= 2*f+1
}
func (n *Node) PBFTViewChangePassed(count int) bool {
	f := (len(n.Config.Peers) - 1) / 3
	return count >= f+1
}
