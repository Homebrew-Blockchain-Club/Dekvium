package pbft

import (
	c "Dekvium/common"
	"fmt"
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

func GenerateQueryKey(msg c.Message) string {
	return fmt.Sprintf("%d%d", msg.View, msg.Sequence)
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
	go j.run()
	return j
}

func (j *journaler) run() {
	for {
		select {
		case entry := <-j.set.RX():
			j.data.Push(entry.key, entry)
			// notify checkpoint
		case query := <-j.get.RX():
			replyChan := query.Second
			data := query.First
			switch data.Type {
			case Key:
				result := []journalEntry{}
				if entry, ok := j.data.GetByKey(data.key); ok {
					result = append(result, entry)
				}
				replyChan <- result
			case SeqRange:
				result := []journalEntry{}
				for _, entry := range j.data.Raw() {
					if entry.seq >= data.seqFrom && entry.seq <= data.seqTo {
						result = append(result, entry)
					}
				}
				replyChan <- result
			}
		case seq := <-j.clean.RX():
			// Garbage collect entries up to and including seq
			for !j.data.IsEmpty() {
				_, entry := j.data.Peek()
				if entry.seq > seq {
					break
				}
				j.data.Pop()
			}
		}
	}
}
