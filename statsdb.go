package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalWorkflowAllocated = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "total_workflow_allocated",
			Help: "Total workflow allocated per namespace.",
		},
		[]string{"namespace"},
	)
)

func init() {
	prometheus.MustRegister(totalWorkflowAllocated)
}

type StatsDB struct {
	namespaces map[string]int
}

func (db *StatsDB) Reset() {
	for k := range db.namespaces {
		db.namespaces[k] = 0
		totalWorkflowAllocated.WithLabelValues(k).Set(0)
	}
}

func (db *StatsDB) Inc(namespace string) {
	totalWorkflowAllocated.WithLabelValues(namespace).Inc()
}

func (db *StatsDB) Remove(namespace string) {
	delete(db.namespaces, namespace)
	totalWorkflowAllocated.DeleteLabelValues(namespace)
}

func (db *StatsDB) Dump() map[string]int {
	return db.namespaces
}

func (db *StatsDB) Get(namespace string) int {
	return db.namespaces[namespace]
}
