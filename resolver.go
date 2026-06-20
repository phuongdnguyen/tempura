package main

import (
	"sort"
	"strconv"

	"github.com/spaolacci/murmur3"
)

type Resolver struct {
	Cache *Mappings
	Stats *StatsDB
}

func NewResolver(cache *Mappings) *Resolver {
	return &Resolver{
		Cache: cache,
		Stats: &StatsDB{namespaces: make(map[string]int)},
	}
}

func (r *Resolver) Resolve(payload *Payload, registry *VirtualNamespaceRegistry) (string, bool) {
	key := payload.WorkflowID
	if physNs := r.Cache.Get(key); physNs != "" {
		// cache hit
		return physNs, true
	}
	virtualNamespace := registry.Resolve(payload.VirtualNamespace)
	physNs := virtualNamespace.Hasher.GetSlot(key)
	r.Stats.Inc(physNs)
	r.Cache.Put(key, physNs)
	return physNs, false
}

type ConsistentHash struct {
	virtualNodes int               // Number of virtual replicas per slot
	ring         []uint32          // Sorted list of vnode hashes
	vnodeToSlot  map[uint32]string // Maps a vnode hash back to the actual slot name
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
	return &ConsistentHash{
		virtualNodes: virtualNodes,
		vnodeToSlot:  make(map[uint32]string),
	}
}

// hash32 converts a string key into a uint32 using Murmur3
func (ch *ConsistentHash) hash32(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

// AddSlot introduces a new slot (node) to the ring
func (ch *ConsistentHash) AddSlot(slot string) {
	for i := 0; i < ch.virtualNodes; i++ {
		// Create a unique name for each virtual node
		vnodeName := slot + "#" + strconv.Itoa(i)
		hash := ch.hash32(vnodeName)

		ch.ring = append(ch.ring, hash)
		ch.vnodeToSlot[hash] = slot
	}
	// Keep the ring sorted to allow binary search
	sort.Slice(ch.ring, func(i, j int) bool { return ch.ring[i] < ch.ring[j] })
}

// GetSlot allocates a key to the nearest clockwise slot
func (ch *ConsistentHash) GetSlot(key string) string {
	if len(ch.ring) == 0 {
		return ""
	}

	hash := ch.hash32(key)

	// Binary search to find the first vnode hash >= key hash
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	// If the key hash is greater than all hashes in the ring,
	// wrap around to the first slot (index 0)
	if idx == len(ch.ring) {
		idx = 0
	}

	return ch.vnodeToSlot[ch.ring[idx]]
}

// RemoveSlot completely deletes a slot and its virtual nodes from the ring
func (ch *ConsistentHash) RemoveSlot(slot string) {
	// 1. Recreate the hashes that were generated for this slot's virtual nodes
	hashesToRemove := make(map[uint32]bool)
	for i := 0; i < ch.virtualNodes; i++ {
		vnodeName := slot + "#" + strconv.Itoa(i)
		hash := ch.hash32(vnodeName)
		hashesToRemove[hash] = true

		// 2. Clean up the map tracking vnode -> slot
		delete(ch.vnodeToSlot, hash)
	}

	// 3. Rebuild the ring slice, filtering out the removed hashes
	newRing := make([]uint32, 0, len(ch.ring)-ch.virtualNodes)
	for _, hash := range ch.ring {
		if !hashesToRemove[hash] {
			newRing = append(newRing, hash)
		}
	}
	ch.ring = newRing
}

// GetAllSlots returns a deduplicated list of all slots (physical namespaces) in the ring
func (ch *ConsistentHash) GetAllSlots() []string {
	uniqueSlots := make(map[string]bool)
	var slots []string
	for _, slot := range ch.vnodeToSlot {
		if !uniqueSlots[slot] {
			uniqueSlots[slot] = true
			slots = append(slots, slot)
		}
	}
	return slots
}
