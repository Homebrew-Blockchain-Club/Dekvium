package storage

import "sync"

type Storage struct {
	kv map[string]string
	mu sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		kv: make(map[string]string),
	}
}
func (s *Storage) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.kv[key]
	if ok {
		return val
	} else {
		return ""
	}
}

func (s *Storage) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = value
}
