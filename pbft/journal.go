package pbft

import (
	c "Dekvium/common"
	"fmt"

	"github.com/sirupsen/logrus"
)

type journalEntry struct {
	seq int
	key string // just concatenate those info
	msg c.Message
}

type journalQueryType int

const (
	Key journalQueryType = iota
	SeqRange
)

type journalQueryData struct {
	Type    journalQueryType
	seqFrom int
	seqTo   int
	key     string
}

func GenerateQueryKey1(msg c.Message) string {
	return fmt.Sprintf("%d|%d%s", msg.View, msg.Sequence, c.MessageType2Str[msg.Type])
}
func GenerateQueryKey2(msg c.Message) string {
	return fmt.Sprintf("%d|%d|%d%s", msg.SenderID, msg.View, msg.Sequence, c.MessageType2Str[msg.Type])
}
func GeneratePrevQueryKey1(msg c.Message, st string) string {
	return fmt.Sprintf("%d|%d%s", msg.View, msg.Sequence, st)
}

type journalQueryPair c.Pair[journalQueryData, chan []journalEntry]

// all chans can be replaced by InfChan to make it fully async
type journaler struct {
	set   *c.InfChan[journalEntry]
	get   *c.InfChan[journalQueryPair]
	clean *c.InfChan[int] // clean until a specific seq number (inclusive)
	data  *c.QueueMap[string, journalEntry]
	node  *Node
}

func NewJournaler(n *Node) *journaler {
	j := &journaler{
		set:   c.NewInfChan[journalEntry](),
		get:   c.NewInfChan[journalQueryPair](),
		clean: c.NewInfChan[int](),
		data:  c.NewJournalQueueMap[string, journalEntry](),
		node:  n,
	}
	logrus.Infof("%s: Journaler initialized for node %v", c.CurFuncName(), n)
	go j.run()
	return j
}

func (j *journaler) run() {
	logrus.Infof("%s: Journaler run loop started", c.CurFuncName())
	for {
		select {
		case entry := <-j.set.RX():
			logrus.WithFields(logrus.Fields{
				"func": c.CurFuncName(),
				"seq":  entry.seq,
				"key":  entry.key,
				"type": entry.msg.Type,
			}).Info("Received journal entry to set")
			j.data.Push(entry.key, entry)
			logrus.WithFields(logrus.Fields{
				"func": c.CurFuncName(),
				"key":  entry.key,
				"seq":  entry.seq,
			}).Debug("Journal entry pushed to data")
			// notify checkpoint
			if j.node != nil && j.node.checkpointFSM != nil && entry.seq == j.node.checkpointFSM.stableCP+100 {
				logrus.WithFields(logrus.Fields{
					"func":     c.CurFuncName(),
					"seq":      entry.seq,
					"stableCP": j.node.checkpointFSM.stableCP,
				}).Info("Notifying checkpoint FSM via haltctl (set branch)")
				select {
				case j.node.checkpointFSM.haltctl <- boot:
					logrus.Infof("%s: Sent boot to checkpointFSM.haltctl", c.CurFuncName())
				default:
					logrus.Warnf("%s: Failed to notify checkpointFSM.haltctl (channel full?)", c.CurFuncName())
				}
			}
		case query := <-j.get.RX():
			logrus.WithFields(logrus.Fields{
				"func":    c.CurFuncName(),
				"type":    query.First.Type,
				"key":     query.First.key,
				"seqFrom": query.First.seqFrom,
				"seqTo":   query.First.seqTo,
			}).Info("Received journal query")
			replyChan := query.Second
			data := query.First
			switch data.Type {
			case Key:
				result := []journalEntry{}
				if entry, ok := j.data.GetByKey(data.key); ok {
					result = append(result, entry)
					logrus.WithFields(logrus.Fields{
						"func":  c.CurFuncName(),
						"key":   data.key,
						"found": true,
					}).Debug("Journal entry found by key")
				} else {
					logrus.WithFields(logrus.Fields{
						"func":  c.CurFuncName(),
						"key":   data.key,
						"found": false,
					}).Debug("No journal entry found by key")
				}
				replyChan <- result
			case SeqRange:
				result := []journalEntry{}
				for _, entry := range j.data.Raw() {
					if entry.seq >= data.seqFrom && entry.seq <= data.seqTo {
						result = append(result, entry)
					}
				}
				logrus.WithFields(logrus.Fields{
					"func":    c.CurFuncName(),
					"seqFrom": data.seqFrom,
					"seqTo":   data.seqTo,
					"count":   len(result),
				}).Debug("Journal entries found by SeqRange")
				replyChan <- result
			}
		case seq := <-j.clean.RX():
			logrus.WithFields(logrus.Fields{
				"func": c.CurFuncName(),
				"seq":  seq,
			}).Info("Received journal clean request")
			// Garbage collect entries up to and including seq
			removed := 0
			for !j.data.IsEmpty() {
				_, entry := j.data.Peek()
				if entry.seq > seq {
					break
				}
				j.data.Pop()
				removed++
			}
			logrus.WithFields(logrus.Fields{
				"func":    c.CurFuncName(),
				"removed": removed,
				"upToSeq": seq,
			}).Info("Journal entries garbage collected")
			// Notify checkpoint FSM if needed
			if j.node != nil && j.node.checkpointFSM != nil && seq == j.node.checkpointFSM.stableCP {
				logrus.WithFields(logrus.Fields{
					"func":     c.CurFuncName(),
					"seq":      seq,
					"stableCP": j.node.checkpointFSM.stableCP,
				}).Info("Notifying checkpoint FSM via haltctl (clean branch)")
				select {
				case j.node.checkpointFSM.haltctl <- boot:
					logrus.Infof("%s: Sent boot to checkpointFSM.haltctl", c.CurFuncName())
				default:
					logrus.Warnf("%s: Failed to notify checkpointFSM.haltctl (channel full?)", c.CurFuncName())
				}
			}
		}
	}
}
