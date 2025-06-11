package pbft

import (
	c "Dekvium/common"
	"Dekvium/config"
	"Dekvium/storage"
	"crypto/ecdsa"
)

// Node is an actor-style FSM node.
type Node struct {
	Config config.Config
	// ID       int
	view     int
	sequence int
	msgCh    chan c.Message
	stopCh   chan struct{}
	respChan chan c.Message
	// peers    []peer
	// peersLock sync.RWMutex
	Storage      *storage.Storage
	privkey      *ecdsa.PrivateKey
	consensusFSM *consensusFSM
	journaler    *journaler
	// unimplemented
	viewchangeFSM struct {
		haltctl chan bootCommand
	}
	// Add more fields as needed (e.g., logs, timers)
	// cvcomm chan bootCommand
	// chjcomm chan
	// cchcomm chan
	// cschan chan
}

// NewNode creates a new PBFT node.
func NewNode(conf config.Config) *Node {
	n := &Node{
		// ID:       id,
		Config:   conf,
		view:     0,
		sequence: 0,
		msgCh:    make(chan c.Message),
		stopCh:   make(chan struct{}),
		respChan: make(chan c.Message),
		// peers:    peers,
		Storage: storage.NewStorage(),
	}
	n.consensusFSM = newConsensusFSM(n)
	n.journaler = NewJournaler(n)
	n.Config.ParsedPrivateKey = ParsePrivateKey(n.Config.PrivateKey)
	for _, peer := range n.Config.Peers {
		peer.ParsedPubKey = ParsePublicKey(peer.PubKey)
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
