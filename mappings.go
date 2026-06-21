package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

type Mappings struct {
	cachev2        *xsync.Map[string, string]
	dataPath       string
	cacheHitCount  int
	cacheMissCount int
	lock           sync.RWMutex
}

func NewMappings(dataPath string) *Mappings {
	if dataPath == "" {
		dataPath = "./data/mappings.json"
	}
	m := &Mappings{
		cachev2: xsync.NewMap[string, string](),
		lock:    sync.RWMutex{},
	}
	ticker := time.NewTicker(10 * time.Second)
	_, err := os.Stat(dataPath)
	if err == nil {
		// read if exist
		file, err := os.ReadFile(dataPath)
		if err != nil {
			log.Fatalf("failed to read mapping file %v, err: %v", dataPath, err)
		}
		data := map[string]string{}
		if err := json.Unmarshal(file, &data); err != nil {
			log.Fatalf("failed to unmarshal mapping file %v, err: %v", dataPath, err)
		}
		log.Println("mapping loading")
		for k, v := range data {
			m.cachev2.Store(k, v)
		}
		log.Println("mapping loaded")
	}

	go func() {
		for {
			select {
			case <-ticker.C:
				//m.lock.Lock()
				data := map[string]string{}
				for k, v := range m.cachev2.All() {
					data[k] = v
				}
				byte, err := json.Marshal(data)
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
	m.lock.RLock()
	if val, ok := m.cachev2.Load(key); ok {
		m.cacheHitCount++
		return val
	}
	m.cacheMissCount++
	m.lock.RUnlock()
	return ""
}

func (m *Mappings) HitCount() int {
	return m.cacheHitCount
}

func (m *Mappings) MissCount() int {
	return m.cacheMissCount
}

func (m *Mappings) Size() int {
	return m.cachev2.Size()
}

func (m *Mappings) Put(key, value string) {
	m.cachev2.Store(key, value)
}

func (m *Mappings) Delete(key string) {
	m.cachev2.Delete(key)
}
