package pbft

import (
	c "Dekvium/common"
	"Dekvium/net"
	"context"
	"time"

	"github.com/looplab/fsm"
	"github.com/sirupsen/logrus"
)

type consensusFSM struct {
	fsm     *fsm.FSM
	node    *Node
	mq      *c.InfChan[c.Message]
	haltctl chan bootCommand
	// nlistenctl chan internalMessage
}

func newConsensusFSM(n *Node) *consensusFSM {
	// nlistenctl := make(chan internalMessage)
	// vlistenctl := make(chan internalMessage)
	mq := c.NewInfChan[c.Message]()
	fsm := fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "preprepare", Src: []string{"idle"}, Dst: "preprepared"},
			{Name: "prepare", Src: []string{"preprepared"}, Dst: "prepared"},
			{Name: "commit", Src: []string{"prepared"}, Dst: "committed"},
			{Name: "reply", Src: []string{"committed"}, Dst: "idle"},
			// {Name: "preprepare-fail", Src: []string{"idle"}, Dst: "halted"},
			// {Name: "prepare-fail", Src: []string{"preprepared"}, Dst: "halted"},
			// {Name: "commit-fail", Src: []string{"prepared"}, Dst: "halted"},
			{Name: "fail", Src: []string{"idle", "preprepared", "prepared"}, Dst: "halted"},
			{Name: "view-change", Src: []string{"halted"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"enter_state": func(_ context.Context, e *fsm.Event) {
				makeMsg := func() c.Message {
					msgType, ok := c.Str2MessageType[e.Dst]
					if !ok {
						c.Boom()
						return c.Message{} // Only broadcast for prepared and committed
					}
					// TODO:  digest should come from earlier preprepare digest
					msg := c.Message{
						Type:     msgType,
						View:     n.view,
						Sequence: n.sequence,
						SenderID: n.Config.ID,
						// Fill other fields as needed
					}
					// TODO: split digest and sign
					// Digest only in req
					// Digest(&msg)
					replyChan := make(chan []journalEntry)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: GenerateQueryKey(msg)}, replyChan}
					reply := <-replyChan
					if len(reply) != 1 {
						c.Boom()
					}
					msg.Digest = reply[0].msg.Digest
					Sign(&msg, n.Config.ParsedPrivateKey)
					return msg
				}
				switch e.Dst {
				// TODO: update current view and sequence
				case "preprepare":
				case "prepare", "commit":
					msg := makeMsg()
					addrs := make([]string, 0)
					for _, peer := range n.Config.Peers {
						addr := peer.Addr
						addrs = append(addrs, addr)
					}
					net.BroadcastMessage(addrs, &msg)
				case "idle":
					if e.Event == "reply" {
						n.respChan <- makeMsg()
					}
					// TODO: when it's viewchange finished, update view
				}
				// Optionally, if entering idle from committed, send reply to client
				// if e.Dst == "idle" && e.Src == "committed" && n.ClientAddr != "" {
				// 	net.SendPBFTMessage(n.ClientAddr, &msg)
				// }
			},
		},
	)
	haltctl := make(chan bootCommand)
	c := &consensusFSM{fsm, n, mq, haltctl}
	c.beginEventLoop()
	// go func() {
	// 	for {
	// 		//halted
	// 		<-n.interfsmctl

	// 		//waiting for released
	// 		<-n.interfsmctl
	// 	}
	// }()
	return c
}
func (co *consensusFSM) beginEventLoop() {
	n := co.node
	fsm := co.fsm
	haltctl := co.haltctl
	mq := co.mq
	go func() {
		// add count to the subroutine,cntChan<-struct{}{}
		// if cntChan send something to this, then fsm enter fail state
		// cntChan := make(chan struct{})
		var cntChan chan struct{}
		timeoutChan := make(chan struct{})
		successChan := make(chan struct{})
		wait := func() {
			cntChan := make(chan struct{})
			defer close(cntChan)
			timeout := time.After(n.Config.ConsensusTimeout)
			cnt := 0
			for {
				select {
				case <-cntChan:
					cnt++
					if n.PBFTConsensusPassed(cnt) {
						successChan <- struct{}{}
						return
					}
				case <-timeout:
					timeoutChan <- struct{}{}
					return
				}
			}
		}
		testcntChan := func() bool {
			select {
			case _, ok := <-cntChan:
				if !ok {
					return false
				} else {
					c.Boom()
					return false
				}
			default:
				return true
			}
		}
		halt := func() {
			fsm.Event(context.Background(), "fail")
			n.viewchangeFSM.haltctl <- boot
			// wait for haltctl send continue message
			<-haltctl
			// trigger view-change event
			fsm.Event(context.Background(), "view-change")
		}
		for {
			select {
			case <-timeoutChan:
				halt()
			case <-successChan:
				for _, e := range fsm.AvailableTransitions() {
					if e != "fail" {
						fsm.Event(context.Background(), e)
						break
					}
				}
			case msg := <-mq.RX():
				// reject unreachable state transition req
				if !fsm.Can(c.MessageType2Str[msg.Type]) {
					logrus.Warnf("%s: received unmatched state transition msg, requested state transfer:%s ignored", c.CurFuncName(), c.MessageType2Str[msg.Type])
					continue
				}
				replicaConsensusProc := func() {
					replyChan := make(chan []journalEntry)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: GenerateQueryKey(msg)}, replyChan}
					reply := <-replyChan
					var journalMatched bool
					if l := len(reply); l == 0 {
						journalMatched = false
					} else if l > 1 {
						c.Boom()
					} else {
						journalMatched = reply[0].msg.Digest == msg.Digest
					}
					if Verify(&msg, n.Config.Peers[msg.SenderID].ParsedPubKey) &&
						journalMatched &&
						msg.View == n.view &&
						msg.Sequence == n.sequence {
						if testcntChan() {
							cntChan <- struct{}{}
						} else {
							// journal only for the first time
							n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: GenerateQueryKey(msg)}, replyChan}
							go wait()
						}
					} else {
						logrus.Warnf("%s: invalid preprepare msg, sent by %d, type:%s", c.CurFuncName(), msg.SenderID, c.MessageType2Str[msg.Type])
					}
				}
				switch fsm.Current() {
				// for primary node, just send the message to yourself
				case "idle":
					replyChan := make(chan []journalEntry)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: GenerateQueryKey(msg)}, replyChan}
					reply := <-replyChan
					var journalMatched bool
					if l := len(reply); l == 0 {
						// TODO: this is false, since the current node should already received the request from client
						journalMatched = true
					} else if l > 1 {
						c.Boom()
					} else {
						journalMatched = reply[0].msg.Digest == msg.Digest
					}
					if msg.SenderID == n.PrimaryNodeNumber() &&
						Verify(&msg, n.Config.Peers[n.PrimaryNodeNumber()].ParsedPubKey) &&
						msg.View == n.view &&
						journalMatched {
						// needs journal
						// if testcntChan() {
						// 	cntChan <- struct{}{}
						// } else {
						// preprepare message should not be broadcasted

						// send journal
						// hey the journal does not seem good...
						n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey(msg), msg}
						// just tell yourself you are going to the preprepared state
						successChan <- struct{}{}
						//go wait()

					} else {
						logrus.Warnf("%s: invalid preprepare msg, sent by %d, type:%s", c.CurFuncName(), msg.SenderID, c.MessageType2Str[msg.Type])
					}
				case "preprepare":
					replicaConsensusProc()
				case "prepare":
					replicaConsensusProc()
				case "commit":
					replicaConsensusProc()
				case "halted":
					c.Boom()
				}
			}
			// case <-nlistenctl: // halted by myself
			// waiting for released by another goroutine, when that goroutine received msg from checkpoint
			// <-nlistenctl
			// }

		}

	}()
}
