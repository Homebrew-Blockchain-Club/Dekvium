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
	mq := c.NewInfChan[c.Message]()
	fsm := fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "preprepare", Src: []string{"idle"}, Dst: "preprepared"},
			{Name: "prepare", Src: []string{"preprepared"}, Dst: "prepared"},
			{Name: "commit", Src: []string{"prepared"}, Dst: "committed"},
			{Name: "reply", Src: []string{"committed"}, Dst: "idle"},
			{Name: "fail", Src: []string{"idle", "preprepared", "prepared"}, Dst: "halted"},
			{Name: "view-change", Src: []string{"halted"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"enter_state": func(_ context.Context, e *fsm.Event) {
				logrus.Infof("%s: Entering state %s from %s", c.CurFuncName(), e.Dst, e.Src)
				makeMsg := func() c.Message {
					msgType, ok := c.Str2MessageType[e.Dst]
					if !ok {
						logrus.Errorf("%s: Unknown message type for state %s", c.CurFuncName(), e.Dst)
						c.Boom()
						return c.Message{}
					}
					msg := c.Message{
						Type:     msgType,
						View:     n.view,
						Sequence: n.sequence,
						SenderID: n.Config.ID,
					}
					logrus.Infof("%s: Creating message type=%v, view=%d, seq=%d, sender=%d", c.CurFuncName(), msg.Type, msg.View, msg.Sequence, msg.SenderID)
					replyChan := make(chan []journalEntry)
					key := GeneratePrevQueryKey1(msg, c.MessageType2Str[msgType-1])
					logrus.Debugf("%s: Querying journal with key=%s", c.CurFuncName(), key)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: key}, replyChan}
					reply := <-replyChan
					logrus.Debugf("%s: Journal query returned %d entries", c.CurFuncName(), len(reply))
					if len(reply) != 1 {
						logrus.Errorf("%s: Journal query for key=%s returned %d entries, expected 1", c.CurFuncName(), key, len(reply))
						c.Boom()
					}
					msg.Digest = reply[0].msg.Digest
					logrus.Debugf("%s: Using digest %s from journal entry", c.CurFuncName(), msg.Digest)
					Sign(&msg, n.Config.ParsedPrivateKey)
					logrus.Infof("%s: Signed message type=%v, digest=%s", c.CurFuncName(), msg.Type, msg.Digest)
					return msg
				}
				switch e.Dst {
				case "preprepare":
					logrus.Infof("%s: Entered preprepare state", c.CurFuncName())
				case "prepare", "commit":
					msg := makeMsg()
					addrs := make([]string, 0)
					for _, peer := range n.Config.Peers {
						addr := peer.Addr
						addrs = append(addrs, addr)
					}
					logrus.Infof("%s: Broadcasting %s message to %d peers", c.CurFuncName(), c.MessageType2Str[msg.Type], len(addrs))
					net.BroadcastMessage(addrs, &msg)
				case "idle":
					logrus.Infof("%s: Entered idle state", c.CurFuncName())
					if e.Event == "reply" {
						logrus.Infof("%s: Sending reply to client", c.CurFuncName())
						n.respChan <- makeMsg()
					}
					// TODO: when it's viewchange finished, update view
				}
			},
		},
	)
	haltctl := make(chan bootCommand)
	logrus.Infof("%s: consensusFSM ready", c.CurFuncName())
	c := &consensusFSM{fsm, n, mq, haltctl}
	c.beginEventLoop()
	return c
}

func (co *consensusFSM) beginEventLoop() {
	n := co.node
	fsm := co.fsm
	haltctl := co.haltctl
	mq := co.mq
	go func() {
		var cntChan chan struct{}
		timeoutChan := make(chan struct{})
		successChan := make(chan struct{})
		wait := func() {
			cntChan = make(chan struct{})
			defer close(cntChan)
			timeout := time.After(n.Config.ConsensusTimeout)
			cnt := 0
			logrus.Infof("%s: wait() started for consensus", c.CurFuncName())
			for {
				select {
				case <-cntChan:
					cnt++
					logrus.Infof("%s: Received consensus vote, cnt=%d", c.CurFuncName(), cnt)
					if n.PBFTConsensusPassed(cnt) {
						logrus.Infof("%s: PBFTConsensusPassed at cnt=%d, signaling successChan", c.CurFuncName(), cnt)
						successChan <- struct{}{}
						return
					}
				case <-timeout:
					logrus.Warnf("%s: Timeout reached in consensus wait()", c.CurFuncName())
					timeoutChan <- struct{}{}
					return
				}
			}
		}
		testcntChan := func() bool {
			select {
			case _, ok := <-cntChan:
				if !ok {
					logrus.Warnf("%s: cntChan closed unexpectedly", c.CurFuncName())
					return false
				} else {
					logrus.Errorf("%s: cntChan received value when not expected", c.CurFuncName())
					c.Boom()
					return false
				}
			default:
				return true
			}
		}
		halt := func() {
			logrus.Warnf("%s: Entering halt() - triggering fail and view-change", c.CurFuncName())
			fsm.Event(context.Background(), "fail")
			n.viewchangeFSM.haltctl <- boot
			logrus.Infof("%s: Waiting for haltctl to continue", c.CurFuncName())
			<-haltctl
			logrus.Infof("%s: Continuing after haltctl, triggering view-change event", c.CurFuncName())
			fsm.Event(context.Background(), "view-change")
		}
		logrus.Infof("%s: consensusFSM event loop started", c.CurFuncName())
		for {
			select {
			case <-timeoutChan:
				logrus.Warnf("%s: TimeoutChan triggered in consensusFSM event loop", c.CurFuncName())
				halt()
			case <-successChan:
				logrus.Infof("%s: SuccessChan triggered, transitioning FSM", c.CurFuncName())
				for _, e := range fsm.AvailableTransitions() {
					if e != "fail" {
						logrus.Infof("%s: FSM event: %s", c.CurFuncName(), e)
						fsm.Event(context.Background(), e)
						break
					}
				}
			case msg := <-mq.RX():
				logrus.Infof("%s: Received message of type %s from %d", c.CurFuncName(), c.MessageType2Str[msg.Type], msg.SenderID)
				if !fsm.Can(c.MessageType2Str[msg.Type]) {
					logrus.Warnf("%s: received unmatched state transition msg, requested state transfer:%s ignored", c.CurFuncName(), c.MessageType2Str[msg.Type])
					continue
				}
				replicaConsensusProc := func() {
					logrus.Debugf("%s: Running replicaConsensusProc for msg type=%s, seq=%d", c.CurFuncName(), c.MessageType2Str[msg.Type], msg.Sequence)
					replyChan := make(chan []journalEntry)
					key := GeneratePrevQueryKey1(msg, c.MessageType2Str[msg.Type-1])
					logrus.Debugf("%s: Querying journal with key=%s", c.CurFuncName(), key)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: key}, replyChan}
					reply := <-replyChan
					var journalMatched bool
					if l := len(reply); l == 0 {
						logrus.Warnf("%s: No journal entry found for key=%s", c.CurFuncName(), key)
						journalMatched = false
					} else if l > 1 {
						logrus.Errorf("%s: Multiple journal entries found for key=%s", c.CurFuncName(), key)
						c.Boom()
					} else {
						journalMatched = reply[0].msg.Digest == msg.Digest
						logrus.Debugf("%s: Journal digest match: %v", c.CurFuncName(), journalMatched)
					}
					if Verify(&msg, n.Config.Peers[msg.SenderID].ParsedPubKey) &&
						journalMatched &&
						msg.View == n.view &&
						msg.Sequence == n.sequence {
						logrus.Infof("%s: Message verified and journal matched, journaling entry", c.CurFuncName())
						n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey2(msg), msg}
						if testcntChan() {
							logrus.Debugf("%s: testcntChan true, sending to cntChan", c.CurFuncName())
							cntChan <- struct{}{}
						} else {
							logrus.Debugf("%s: testcntChan false, journaling for first time and starting wait()", c.CurFuncName())
							n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey1(msg), msg}
							go wait()
							cntChan <- struct{}{}
						}
					} else {
						logrus.Warnf("%s: invalid preprepare msg, sent by %d, type:%s", c.CurFuncName(), msg.SenderID, c.MessageType2Str[msg.Type])
					}
				}
				switch fsm.Current() {
				case "idle":
					logrus.Infof("%s: FSM in idle state, processing message", c.CurFuncName())
					replyChan := make(chan []journalEntry)
					key := GeneratePrevQueryKey1(msg, c.MessageType2Str[msg.Type-1])
					logrus.Debugf("%s: Querying journal with key=%s", c.CurFuncName(), key)
					n.journaler.get.TX() <- journalQueryPair{journalQueryData{Type: Key, key: key}, replyChan}
					reply := <-replyChan
					var journalMatched bool
					if l := len(reply); l == 0 {
						logrus.Warnf("%s: No journal entry found for key=%s", c.CurFuncName(), key)
						journalMatched = true // TODO: this is false, since the current node should already received the request from client
					} else if l > 1 {
						logrus.Errorf("%s: Multiple journal entries found for key=%s", c.CurFuncName(), key)
						c.Boom()
					} else {
						journalMatched = reply[0].msg.Digest == msg.Digest
						logrus.Debugf("%s: Journal digest match: %v", c.CurFuncName(), journalMatched)
					}
					if msg.SenderID == n.PrimaryNodeNumber() &&
						Verify(&msg, n.Config.Peers[n.PrimaryNodeNumber()].ParsedPubKey) &&
						msg.View == n.view &&
						journalMatched {
						logrus.Infof("%s: Primary node message verified and journal matched, journaling entry", c.CurFuncName())
						n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey1(msg), msg}
						successChan <- struct{}{}
					} else {
						logrus.Warnf("%s: invalid preprepare msg, sent by %d, type:%s", c.CurFuncName(), msg.SenderID, c.MessageType2Str[msg.Type])
					}
				case "preprepare":
					logrus.Infof("%s: FSM in preprepare state, running replicaConsensusProc", c.CurFuncName())
					replicaConsensusProc()
				case "prepare":
					logrus.Infof("%s: FSM in prepare state, running replicaConsensusProc", c.CurFuncName())
					replicaConsensusProc()
				case "commit":
					logrus.Infof("%s: FSM in commit state, running replicaConsensusProc", c.CurFuncName())
					replicaConsensusProc()
				case "halted":
					logrus.Errorf("%s: FSM in halted state, should not process messages", c.CurFuncName())
					c.Boom()
				}
			}
		}
	}()
}
