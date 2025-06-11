package common

type LinearDS[T any] interface {
	Push(T)
	Pop() T
	Peek() T
	IsEmpty() bool
	Length() int
}

// this DS does not provide thread security
type Stack[T any] struct {
	items []T
}

func NewStack[T any]() *Stack[T] {
	return &Stack[T]{
		items: make([]T, 0),
	}
}

func (s *Stack[T]) Push(item T) {
	s.items = append(s.items, item)
}
func (s *Stack[T]) Pop() T {
	l := len(s.items)
	if l == 0 {
		panic("Stack: pop")
	}
	ret := s.items[l-1]
	s.items = s.items[:l-1]
	return ret
}
func (s Stack[T]) Peek() T {
	l := len(s.items)
	if l == 0 {
		panic("Stack: peek")
	}
	return s.items[l]
}
func (s Stack[T]) IsEmpty() bool {
	return len(s.items) == 0
}
func (s Stack[T]) Length() int {
	return len(s.items)
}

type op int

const (
	ADD op = iota
	DELETE
	CLOSE
)

type Queue[T any] struct {
	items []T
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		items: make([]T, 0),
	}
}

func (s *Queue[T]) Push(item T) {
	s.items = append(s.items, item)
}
func (s *Queue[T]) Pop() T {
	l := len(s.items)
	if l == 0 {
		panic("Queue: pop")
	}
	ret := s.items[0]
	s.items = s.items[1:]
	return ret
}
func (s Queue[T]) Peek() T {
	l := len(s.items)
	if l == 0 {
		panic("Queue: peek")
	}
	return s.items[l]
}
func (s Queue[T]) IsEmpty() bool {
	return len(s.items) == 0
}
func (s Queue[T]) Length() int {
	return len(s.items)
}

type InfChan[T any] struct {
	tx, rx chan T
}

func (c InfChan[T]) TX() chan<- T {
	return c.tx
}
func (c InfChan[T]) RX() <-chan T {
	return c.rx
}
func NewInfChan[T any]() *InfChan[T] {
	tx := make(chan T)
	rx := make(chan T)
	q := NewQueue[T]()
	// go func() {
	// 	for {
	// 		e := <-tx
	// 		// create new goroutine, if queue.size() is originally 0,
	// 		// waiting to be consumed
	// 		q.Push(e)
	// 		if q.Length() == 1 {
	// 			go func() {
	// 				for {
	// 					rx <- q.Pop()
	// 					// if the queue is empty, then exit, rx "closed"
	// 					if q.IsEmpty() {
	// 						return
	// 					}
	// 				}
	// 			}()
	// 		}
	// 	}
	// }()
	go func() {
		for {
			var out chan T
			// if the q is empty then out would not be open
			if !q.IsEmpty() {
				out = rx
			}
			select {
			case e := <-tx:
				q.Push(e)
			// if no one is listening to rx then this would not run
			case out <- q.Peek():
				q.Pop()
			}
		}
	}()
	return &InfChan[T]{tx, rx}
}

type QueueMap[K comparable, V any] struct {
	queue []K     // FIFO queue of entries
	store map[K]V // key -> index in queue
}

// NewJournalQueueMap initializes a new JournalQueueMap.
func NewJournalQueueMap[K comparable, V any]() *QueueMap[K, V] {
	return &QueueMap[K, V]{
		queue: make([]K, 0),
		store: make(map[K]V),
	}
}

// Push adds a new entry to the queue and map.
func (j *QueueMap[K, V]) Push(key K, val V) {
	j.queue = append(j.queue, key)
	if _, ok := j.store[key]; ok {
		panic("QueueMap: duplicated key")
	}
	j.store[key] = val
}

// Pop removes the oldest entry from the queue and map.
func (j *QueueMap[K, V]) Pop() (K, V) {
	if len(j.queue) == 0 {
		panic("QueueMap: pop")
	}
	entry := j.queue[0]
	j.queue = j.queue[1:]
	val, _ := j.store[entry]
	delete(j.store, entry)
	// Update indices for remaining entries
	return entry, val
}
func (s QueueMap[K, V]) IsEmpty() bool {
	return len(s.queue) == 0
}

// GetByKey returns the entry for a given key, if present.
func (j *QueueMap[K, V]) GetByKey(key K) (val V, ok bool) {
	val, ok = j.store[key]
	return
}
func (qm *QueueMap[K, V]) Length() int {
	return len(qm.queue)
}
func (qm *QueueMap[K, V]) Peek() (K, V) {
	return qm.queue[0], qm.store[qm.queue[0]]
}
func (qm *QueueMap[K, V]) Raw() map[K]V {
	return qm.store
}

type Pair[A, B any] struct {
	First  A
	Second B
}
