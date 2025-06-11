package pbft

import (
	c "Dekvium/common"
	"Dekvium/net"
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"
	"github.com/sirupsen/logrus"
)

type checkpointFSM struct {
	fsm      *fsm.FSM
	node     *Node
	haltctl  chan bootCommand
	stableCP int
	mq       *c.InfChan[c.Message]
}

func (ch checkpointFSM) digestCheckpoint(msg *c.Message) {
	msg.Digest = fmt.Sprintf("%d|%d", ch.node.view, ch.node.sequence)
}
func (ch checkpointFSM) checkDigest(msg *c.Message) bool {
	var view, seq int
	fmt.Sscanf(msg.Digest, "%d|%d", &view, &seq)
	return view == ch.node.view && seq == ch.node.sequence
}
func newCheckpointFSM(n *Node) *checkpointFSM {
	ch := &checkpointFSM{node: n}
	fsm := fsm.NewFSM(
		"idle",
		fsm.Events{
			{Name: "start", Src: []string{"idle"}, Dst: "checkpoint"},
			{Name: "new-checkpoint", Src: []string{"checkpoint"}, Dst: "idle"},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				makeMsg := func() c.Message {
					msgType, ok := c.Str2MessageType[e.Dst]
					if !ok {
						c.Boom()
						return c.Message{}
					}
					msg := c.Message{
						Type:     msgType,
						Sequence: n.sequence,
						SenderID: n.Config.ID,
					}
					ch.digestCheckpoint(&msg)
					Sign(&msg, n.Config.ParsedPrivateKey)
					return msg
				}
				switch e.Dst {
				case "checkpoint":
					msg := makeMsg()
					addrs := make([]string, 0)
					for _, peer := range n.Config.Peers {
						addr := peer.Addr
						addrs = append(addrs, addr)
					}
					net.BroadcastMessage(addrs, &msg)
					ch.stableCP = n.sequence
				case "idle":
					n.journaler.clean.TX() <- ch.stableCP - 1
				}

			},
		},
	)
	haltctl := make(chan bootCommand)
	logrus.Infof("%s: ready", c.CurFuncName())
	ch.haltctl = haltctl
	ch.fsm = fsm
	ch.beginEventLoop()
	ch.mq = c.NewInfChan[c.Message]()
	return ch
}
func (ch *checkpointFSM) beginEventLoop() {
	n := ch.node
	fsm := ch.fsm
	haltctl := ch.haltctl
	mq := ch.mq
	go func() {
		var cntChan chan struct{}
		timeoutChan := make(chan struct{})
		successChan := make(chan struct{})
		wait := func() {
			cntChan := make(chan struct{})
			defer close(cntChan)
			timeout := time.After(n.Config.ConsensusTimeout)
			cnt := 0
			logrus.Infof("%s: wait() started for checkpoint consensus", c.CurFuncName())
			for {
				select {
				case <-cntChan:
					cnt++
					logrus.Infof("%s: Received checkpoint vote, cnt=%d", c.CurFuncName(), cnt)
					if n.PBFTConsensusPassed(cnt) {
						logrus.Infof("%s: PBFTConsensusPassed at cnt=%d, signaling successChan", c.CurFuncName(), cnt)
						successChan <- struct{}{}
						return
					}
				case <-timeout:
					logrus.Warnf("%s: Timeout reached in checkpoint wait()", c.CurFuncName())
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
		logrus.Infof("%s: checkpointFSM event loop started", c.CurFuncName())
		for {
			select {
			case <-timeoutChan:
				logrus.Warnf("%s: TimeoutChan triggered in checkpointFSM event loop", c.CurFuncName())
				// never mind
			case <-successChan:
				logrus.Infof("%s: SuccessChan triggered, transitioning FSM", c.CurFuncName())
				for _, e := range fsm.AvailableTransitions() {
					logrus.Infof("%s: FSM event: %s", c.CurFuncName(), e)
					fsm.Event(context.Background(), e)
				}
				// event: new-checkpoint
			case <-haltctl:
				logrus.Infof("%s: Haltctl triggered, transitioning FSM", c.CurFuncName())
				for _, e := range fsm.AvailableTransitions() {
					logrus.Infof("%s: FSM event: %s", c.CurFuncName(), e)
					fsm.Event(context.Background(), e)
				}
				// event: start
			case msg := <-mq.RX():
				logrus.Infof("%s: Received message of type %s from %d", c.CurFuncName(), c.MessageType2Str[msg.Type], msg.SenderID)
				if !fsm.Can(c.MessageType2Str[msg.Type]) {
					logrus.Warnf("%s: received unmatched state transition msg, requested state transfer:%s ignored", c.CurFuncName(), c.MessageType2Str[msg.Type])
					continue
				}
				if Verify(&msg, n.Config.Peers[msg.SenderID].ParsedPubKey) &&
					ch.checkDigest(&msg) {
					logrus.Infof("%s: Message verified and digest matched, journaling entry", c.CurFuncName())
					n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey2(msg), msg}
					if testcntChan() {
						logrus.Infof("%s: testcntChan true, sending to cntChan", c.CurFuncName())
						cntChan <- struct{}{}
					} else {
						logrus.Infof("%s: testcntChan false, starting wait() and sending to cntChan", c.CurFuncName())
						go wait()
						cntChan <- struct{}{}
					}
				} else {
					logrus.Warnf("%s: Message verification or digest check failed", c.CurFuncName())
				}
			}
		}
	}()
}
