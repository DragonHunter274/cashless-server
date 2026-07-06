package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PurchaseCollector struct {
	purchaseDesc  *prometheus.Desc
	creationTimes map[string]time.Time // Cache creation timestamps
	mu            sync.RWMutex
}

func NewPurchaseCollector() *PurchaseCollector {
	return &PurchaseCollector{
		purchaseDesc: prometheus.NewDesc(
			"purchases_total",
			"Total number of confirmed purchases",
			[]string{"product", "machine_id", "method"},
			nil,
		),
		creationTimes: make(map[string]time.Time),
	}
}

// Add this missing method
func (c *PurchaseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.purchaseDesc
}

func (c *PurchaseCollector) getOrSetCreationTime(product, machineID, method string, firstTransactionTime time.Time) time.Time {
	key := fmt.Sprintf("%s|%s|%s", product, machineID, method)
	c.mu.RLock()
	if createdAt, exists := c.creationTimes[key]; exists {
		c.mu.RUnlock()
		return createdAt
	}
	c.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check after acquiring write lock
	if createdAt, exists := c.creationTimes[key]; exists {
		return createdAt
	}
	// Set and cache the creation time (use the first transaction time)
	c.creationTimes[key] = firstTransactionTime
	return firstTransactionTime
}

func (c *PurchaseCollector) Collect(ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	type MetricResult struct {
		Product        string
		MachineID      string
		Method         string
		Count          int64
		FirstCreatedAt time.Time
	}

	var results []MetricResult
	err := db.Raw(`
        SELECT
            COALESCE(product, '') as product,
            COALESCE(machine_id, '') as machine_id,
            CASE
                WHEN is_cash = true THEN 'cash'
                ELSE COALESCE(payment_method, 'unknown')
            END as method,
            COUNT(*) as count,
            MIN(created_at) as first_created_at
        FROM transactions
        WHERE status = 'confirmed'
        AND amount < 0
        GROUP BY product, machine_id,
            (CASE WHEN is_cash = true THEN 'cash' ELSE COALESCE(payment_method, 'unknown') END)
    `).Scan(&results).Error

	if err != nil {
		log.Printf("Error querying purchase metrics: %v", err)
		return
	}

	for _, result := range results {
		// Get stable creation timestamp (cached after first encounter)
		creationTime := c.getOrSetCreationTime(result.Product, result.MachineID, result.Method, result.FirstCreatedAt)
		metric, err := prometheus.NewConstMetricWithCreatedTimestamp(
			c.purchaseDesc,
			prometheus.CounterValue,
			float64(result.Count),
			creationTime,
			result.Product, result.MachineID, result.Method,
		)
		if err != nil {
			log.Printf("Error creating metric: %v", err)
			continue
		}
		ch <- metric
	}
}

func init() {
	prometheus.MustRegister(NewPurchaseCollector())
}
