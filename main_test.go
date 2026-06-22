package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_All(t *testing.T) {
	//create phys namespaces
	namespaces := []string{"phys-ns-1", "phys-ns-2"}
	// create virtual namespace
	const vNamespaceName = "my-virtual-namespace"
	vNamespace := NewVirtualNamespace(vNamespaceName)
	for _, namespace := range namespaces {
		vNamespace.Add(&Namespace{
			cluster: nil,
			name:    namespace,
		})
	}

	// create cache
	mappings := NewMappings("localhost:6379")
	mappings.Clear()
	resolver := NewResolver(mappings)

	registry := NewVirtualNamespaceRegistry("./data/namespace-registry.json")
	registry.Register(vNamespace)

	payloads := makePayloads(1000, vNamespaceName)

	tests := []struct {
		name      string
		test      func()
		assertion func(t *testing.T)
	}{
		{
			"fresh payloads",
			func() {
				// generate payload
				for _, payload := range payloads {
					resolver.Resolve(payload, registry)
				}
			},
			func(t *testing.T) {
				assert.Equal(t, 1000, mappings.Size())
			},
		},
		{
			"add a namespace and try to resolve an old payload",
			func() {},
			func(t *testing.T) {
				// re-resolve an old payload
				physNamespace, _ := resolver.Resolve(payloads[0], registry)
				assert.NotEqualf(t, "phys-ns-3", physNamespace, "old payload should stick to existing namespaces")
			},
		},
		{
			"load 3k more payloads",
			func() {
				toAdd := &Namespace{
					cluster: nil,
					name:    "my-phys-ns",
				}
				//	Add a namespace
				vNamespace.Add(toAdd)
				payloads = append(payloads, makePayloads(3000, vNamespaceName)...)
				for _, payload := range payloads {
					resolver.Resolve(payload, registry)
				}
			},
			func(t *testing.T) {
			},
		},
		{
			"removing a physical namespace from its virtual group",
			func() {
				toRemove := &Namespace{
					cluster: nil,
					name:    "my-phys-ns",
				}
				//	Remove a namespace
				vNamespace.Remove(toRemove)
			},
			func(t *testing.T) {
				payloads = append(payloads, makePayloads(3000, vNamespaceName)...)
				currentAllocatedToCordoned := resolver.Stats.Get("my-phys-ns")
				actualCordonedHit := 0
				for _, payload := range payloads {
					physNamespace, hit := resolver.Resolve(payload, registry)
					if physNamespace == "my-phys-ns" {
						assert.Truef(t, hit, "existing payloads should stick to cordoned physical namespaces")
						actualCordonedHit++
					}
					if !hit {
						assert.NotEqualf(t, "my-phys-ns", physNamespace, "new payload should not go to removed physical namespaces")
					}

				}
				assert.Equalf(t, currentAllocatedToCordoned, actualCordonedHit, "old payload should stick to cordoned physical namespaces")
			},
		},
		{
			"add the namespace back",
			func() {
				vNamespace.Add(&Namespace{
					name: "my-phys-ns",
				})
			},
			func(t *testing.T) {
				payloads = append(payloads, makePayloads(3000, vNamespaceName)...)
				for _, payload := range payloads {
					resolver.Resolve(payload, registry)
				}
			},
		},
	}

	for _, tt := range tests {
		tt.test()
		tt.assertion(t)
	}
}
