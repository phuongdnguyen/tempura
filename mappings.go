package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type Mappings struct {
	client         *redis.Client
	cacheHitCount  int
	cacheMissCount int
	ctx            context.Context
}

func NewMappings(redisAddr string) *Mappings {
	if redisAddr == "" || redisAddr == "./data/mappings.json" {
		redisAddr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Printf("Warning: failed to connect to redis at %s: %v", redisAddr, err)
	} else {
		log.Printf("Connected to redis at %s", redisAddr)
	}

	return &Mappings{
		client: client,
		ctx:    ctx,
	}
}

func (m *Mappings) Get(key string) string {
	val, err := m.client.Get(m.ctx, key).Result()
	if err == redis.Nil || err != nil {
		m.cacheMissCount++
		return ""
	}
	m.cacheHitCount++
	return val
}

func (m *Mappings) Put(key, value string) {
	m.client.Set(m.ctx, key, value, 0)
}

func (m *Mappings) Delete(key string) {
	m.client.Del(m.ctx, key)
}

func (m *Mappings) Size() int {
	size, _ := m.client.DBSize(m.ctx).Result()
	return int(size)
}

func (m *Mappings) HitCount() int {
	return m.cacheHitCount
}

func (m *Mappings) MissCount() int {
	return m.cacheMissCount
}

func (m *Mappings) Clear() {
	m.client.FlushDB(m.ctx)
}
