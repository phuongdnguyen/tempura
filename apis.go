package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type CreateVirtualNamespaceRequest struct {
	Name string `json:"name"`
}

type ManagePhysicalNamespaceRequest struct {
	VirtualNamespace  string `json:"virtual_namespace"`
	PhysicalNamespace string `json:"physical_namespace"`
	ClusterAddress    string `json:"cluster_address"`
}

func startAdminServer(port int, registry *VirtualNamespaceRegistry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/namespace/virtual/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateVirtualNamespaceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Name == "" {
			http.Error(w, "name is required", http.StatusBadRequest)
			return
		}

		if registry.Resolve(req.Name) != nil {
			http.Error(w, "virtual namespace already exists", http.StatusConflict)
			return
		}

		vNs := NewVirtualNamespace(req.Name)
		registry.Register(vNs)

		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Virtual namespace '%s' created successfully\n", req.Name)
	})

	mux.HandleFunc("/namespace/physical/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ManagePhysicalNamespaceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.VirtualNamespace == "" || req.PhysicalNamespace == "" || req.ClusterAddress == "" {
			http.Error(w, "virtual_namespace, physical_namespace, and cluster_address are required", http.StatusBadRequest)
			return
		}

		vNs := registry.Resolve(req.VirtualNamespace)
		if vNs == nil {
			http.Error(w, "virtual namespace not found", http.StatusNotFound)
			return
		}

		formattedName := buildPhysicalNamespace(req.PhysicalNamespace, req.ClusterAddress)
		vNs.Add(&Namespace{name: formattedName})

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Physical namespace '%s' added to virtual namespace '%s'\n", req.PhysicalNamespace, req.VirtualNamespace)
	})

	mux.HandleFunc("/namespace/physical/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req ManagePhysicalNamespaceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.VirtualNamespace == "" || req.PhysicalNamespace == "" || req.ClusterAddress == "" {
			http.Error(w, "virtual_namespace, physical_namespace, and cluster_address are required", http.StatusBadRequest)
			return
		}

		vNs := registry.Resolve(req.VirtualNamespace)
		if vNs == nil {
			http.Error(w, "virtual namespace not found", http.StatusNotFound)
			return
		}

		formattedName := buildPhysicalNamespace(req.PhysicalNamespace, req.ClusterAddress)
		vNs.Remove(&Namespace{name: formattedName})

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Physical namespace '%s' removed from virtual namespace '%s'\n", req.PhysicalNamespace, req.VirtualNamespace)
	})

	mux.HandleFunc("/namespace/virtual/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "name query parameter is required", http.StatusBadRequest)
			return
		}

		vNs := registry.Resolve(name)
		if vNs == nil {
			http.Error(w, "virtual namespace not found", http.StatusNotFound)
			return
		}

		namespacesWithStatus := vNs.GetAllNamespacesWithStatus()

		response := map[string]interface{}{
			"name":                name,
			"physical_namespaces": namespacesWithStatus,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting Admin API Server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Failed to start admin server: %v", err)
	}
}
