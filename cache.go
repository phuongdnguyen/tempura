package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

// TODO: thread-safe cache
type Mappings struct {
	cache          map[string]string
	dataPath       string
	cacheHitCount  int
	cacheMissCount int
	lock           sync.Mutex
}

func NewMappings(dataPath string) *Mappings {
	if dataPath == "" {
		dataPath = "./data/mappings.json"
	}
	m := &Mappings{
		cache: make(map[string]string),
	}
	ticker := time.NewTicker(10 * time.Second)
	_, err := os.Stat(dataPath)
	if err == nil {
		// read if exist
		file, err := os.ReadFile(dataPath)
		if err != nil {
			log.Fatalf("failed to read mapping file %v, err: %v", dataPath, err)
		}
		if err := json.Unmarshal(file, &m.cache); err != nil {
			log.Fatalf("failed to unmarshal mapping file %v, err: %v", dataPath, err)
		}
	}

	go func() {
		for {
			select {
			case <-ticker.C:
				//m.lock.Lock()
				byte, err := json.Marshal(m.cache)
				if err != nil {
					log.Fatalf("failed to marshal cache, err: %v", err)
				}
				if err := os.WriteFile(dataPath, byte, os.ModePerm); err != nil {
					log.Fatalf("failed to checkpoint cache, err: %v", err)
				}
				//m.lock.Unlock()
				log.Printf("cache checkpointed at: %s", time.Now().UTC().Format(time.RFC3339))
			}
		}
	}()
	return m
}

func (m *Mappings) Get(key string) string {
	//m.lock.Lock()
	if val, ok := m.cache[key]; ok {
		m.cacheHitCount++
		return val
	}
	m.cacheMissCount++
	//m.lock.Unlock()
	return ""
}

func (m *Mappings) HitCount() int {
	return m.cacheHitCount
}

func (m *Mappings) MissCount() int {
	return m.cacheMissCount
}

func (m *Mappings) Size() int {
	return len(m.cache)
}

func (m *Mappings) Put(key, value string) {
	//m.lock.Lock()
	(*m).cache[key] = value
	//m.lock.Lock()
}
