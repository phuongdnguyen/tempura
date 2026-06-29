package main

import (
	"flag"
	"log"
)

func main() {
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis server address")
	listenAddr := flag.String("listen-addr", "localhost:8088", "Proxy listen address")
	adminPort := flag.Int("admin-port", 8089, "Admin API port")
	defaultTarget := flag.String("default-target", "http://localhost:7233", "Default target address")
	flag.Parse()

	registry := NewVirtualNamespaceRegistry(*redisAddr)

	// Start the Admin API on port 8089
	go startAdminServer(*adminPort, registry)

	proxy, err := NewStickyProxy(*listenAddr, *redisAddr, *defaultTarget, registry)
	if err != nil {
		log.Fatal("Error creating proxy: ", err)
	}
	proxy.Start()
}
