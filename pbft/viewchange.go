package pbft

import (
	c "Dekvium/common"
	"context"
	"fmt"
	"time"

	"github.com/looplab/fsm"
	"github.com/sirupsen/logrus"
)

type viewchangeFSM struct {
	fsm     *fsm.FSM
	node    *Node
	haltctl chan bootCommand
	mq      *c.InfChan[c.Message]
}

func newViewChangeFSM(n *Node) *viewchangeFSM {
	mq := c.NewInfChan[c.Message]()
	fsmObj := fsm.NewFSM(
		"halted",
		fsm.Events{
			{Name: "initialize", Src: []string{"halted"}, Dst: "starting"},
			{Name: "view-change", Src: []string{"starting"}, Dst: "view-changing"},
			{Name: "new-view", Src: []string{"view-changing"}, Dst: "halted"},
		},
		fsm.Callbacks{
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				logrus.Infof("%s: Entering state %s from %s", c.CurFuncName(), e.Dst, e.Src)
				// capture for closure
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
					// For view-change, digest can be view|seq or more info as needed
					msg.Digest = fmt.Sprintf("%d|%d", n.view, n.sequence)
					Sign(&msg, n.Config.ParsedPrivateKey)
					logrus.Infof("%s: Created view-change message: type=%v, view=%d, seq=%d, sender=%d, digest=%s", c.CurFuncName(), msg.Type, msg.View, msg.Sequence, msg.SenderID, msg.Digest)
					return msg
				}
				switch e.Dst {
				case "view-changing":
					logrus.Infof("%s: Collecting journal entries for view-change, seq window [%d, %d]", c.CurFuncName(), max(0, n.sequence-10), n.sequence)
					seqFrom := max(0, n.sequence-10) // or whatever window you want
					seqTo := n.sequence
					replyChan := make(chan []journalEntry)
					query := journalQueryPair{
						First: journalQueryData{
							Type:    SeqRange,
							seqFrom: seqFrom,
							seqTo:   seqTo,
						},
						Second: replyChan,
					}
					n.journaler.get.TX() <- query
					entries := <-replyChan
					logrus.Infof("%s: Collected %d journal entries for view-change", c.CurFuncName(), len(entries))
					msg := makeMsg()
					addrs := make([]string, 0)
					for _, peer := range n.Config.Peers {
						addrs = append(addrs, peer.Addr)
					}
					logrus.Infof("%s: Prepared to broadcast view-change message to %d peers", c.CurFuncName(), len(addrs))
					// net.BroadcastMessage(addrs, &msg) // Uncomment if net is imported
					n.journaler.set.TX() <- journalEntry{n.sequence, GenerateQueryKey1(msg), msg}
					logrus.Infof("%s: Journaled view-change message for seq=%d", c.CurFuncName(), n.sequence)
				case "halted":
					logrus.Infof("%s: Entered halted state, cleaning up if necessary", c.CurFuncName())
					// Optionally, cleanup or reset state
				}
			},
		},
	)
	haltctl := make(chan bootCommand)
	logrus.Infof("%s: viewchangeFSM ready", c.CurFuncName())
	vc := &viewchangeFSM{
		fsm:     fsmObj,
		node:    n,
		haltctl: haltctl,
		mq:      mq,
	}
	vc.beginEventLoop()
	return vc
}

func (vc *viewchangeFSM) beginEventLoop() {
	n := vc.node
	fsm := vc.fsm
	haltctl := vc.haltctl
	mq := vc.mq
	go func() {
		timeoutChan := make(chan struct{})
		successChan := make(chan struct{})
		var cntChan chan struct{}
		wait := func() {
			cntChan = make(chan struct{})
			defer close(cntChan)
			timeout := time.After(n.Config.ConsensusTimeout)
			cnt := 0
			logrus.Infof("%s: viewchangeFSM wait() started", c.CurFuncName())
			for {
				select {
				case <-cntChan:
					cnt++
					logrus.Infof("%s: Received view-change vote, cnt=%d", c.CurFuncName(), cnt)
					if n.PBFTViewChangePassed(cnt) {
						logrus.Infof("%s: PBFTViewChangePassed at cnt=%d, signaling successChan", c.CurFuncName(), cnt)
						successChan <- struct{}{}
						return
					}
				case <-timeout:
					logrus.Warnf("%s: Timeout reached in viewchange wait()", c.CurFuncName())
					timeoutChan <- struct{}{}
					return
				}
			}
		}
		logrus.Infof("%s: viewchangeFSM event loop started", c.CurFuncName())
		for {
			select {
			case <-timeoutChan:
				logrus.Warnf("%s: View-change timeout, possibly trigger another view-change or escalate", c.CurFuncName())
				// haltctl <- boot // notify main FSM or supervisor
			case <-successChan:
				logrus.Infof("%s: Enough view-change votes, transitioning to new-view", c.CurFuncName())
				fsm.Event(context.Background(), "new-view")
				// haltctl <- boot // notify main FSM or supervisor
			case <-haltctl:
				logrus.Infof("%s: Haltctl triggered, starting view-change if possible", c.CurFuncName())
				if fsm.Can("view-change") {
					logrus.Infof("%s: FSM can do view-change, firing event and starting wait()", c.CurFuncName())
					fsm.Event(context.Background(), "view-change")
					go wait()
				} else {
					logrus.Warnf("%s: FSM cannot do view-change from current state: %s", c.CurFuncName(), fsm.Current())
				}
			case msg := <-mq.RX():
				logrus.Infof("%s: Received message of type %s from %d", c.CurFuncName(), c.MessageType2Str[msg.Type], msg.SenderID)
				if !fsm.Can(c.MessageType2Str[msg.Type]) {
					logrus.Warnf("%s: received unmatched state transition msg, requested state transfer:%s ignored", c.CurFuncName(), c.MessageType2Str[msg.Type])
					continue
				}
				// Verify view-change message
				if Verify(&msg, n.Config.Peers[msg.SenderID].ParsedPubKey) {
					logrus.Infof("%s: View-change message verified from %d, journaling", c.CurFuncName(), msg.SenderID)
					n.journaler.set.TX() <- journalEntry{msg.Sequence, GenerateQueryKey2(msg), msg}
					if cntChan != nil {
						logrus.Infof("%s: Sending to cntChan for view-change vote", c.CurFuncName())
						cntChan <- struct{}{}
					}
				} else {
					logrus.Warnf("%s: View-change message verification failed from %d", c.CurFuncName(), msg.SenderID)
				}
			}
		}
	}()
}

func (vc *viewchangeFSM) collectJournalEntries(view int, seqFrom, seqTo int) []journalEntry {
	replyChan := make(chan []journalEntry)
	query := journalQueryPair{
		First: journalQueryData{
			Type:    SeqRange,
			seqFrom: seqFrom,
			seqTo:   seqTo,
		},
		Second: replyChan,
	}
	vc.node.journaler.get.TX() <- query
	entries := <-replyChan
	return entries
}

// Helper function for max
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
