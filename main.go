package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GORM Models
type User struct {
	UID       string    `gorm:"primaryKey" json:"uid"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

type UserMachinePrivilege struct {
	UID       string `gorm:"primaryKey" json:"uid"`
	MachineID string `gorm:"primaryKey" json:"machine_id"`
	FreeVend  bool   `gorm:"default:false" json:"free_vend"`
}

type VendVoucher struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	UID       string    `json:"uid"`
	MachineID string    `json:"machine_id"`
	Used      bool      `gorm:"default:false" json:"used"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

type TransactionModel struct {
	ID            uint       `gorm:"primaryKey" json:"transaction_id"`
	UID           *string    `json:"uid"`
	Amount        int        `json:"amount"`
	Product       string     `json:"product"`
	Status        string     `json:"status"`
	PaymentMethod string     `gorm:"column:payment_method" json:"payment_method"`
	MachineID     string     `gorm:"column:machine_id" json:"machine_id"`
	IsCash        bool       `gorm:"default:false" json:"is_cash"`
	CreatedAt     time.Time  `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (TransactionModel) TableName() string {
	return "transactions"
}

type APIKey struct {
	Key              string    `gorm:"primaryKey" json:"key"`
	AllowedEndpoints string    `json:"allowed_endpoints"`
	CreatedAt        time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

type ProductMap struct {
	ID          int    `gorm:"primaryKey" json:"id"`
	ProductName string `json:"product_name"`
}

// Request/Response structs
type Transaction struct {
	ID        int       `json:"transaction_id"`
	UID       *string   `json:"uid"`
	Amount    int       `json:"amount"`
	Product   string    `json:"product"`
	Status    string    `json:"status"`
	Method    string    `json:"payment_method"`
	MachineID string    `json:"machine_id"`
	CreatedAt time.Time `json:"created_at"`
}

type CashPurchase struct {
	Amount    int    `json:"amount"`
	Product   int    `json:"product"`
	MachineID string `json:"machine_id"`
}

type TopUpRequest struct {
	UID    string `json:"uid"`
	Amount int    `json:"amount"`
}

type PurchaseRequest struct {
	UID       *string `json:"uid"`
	Amount    int     `json:"amount"`
	Product   int     `json:"product"`
	MachineID string  `json:"machine_id"`
}

type ConfirmRequest struct {
	TransactionID int `json:"transaction_id"`
}

type BalanceRequest struct {
	UID string `json:"uid"`
}

type Balance struct {
	Balance int    `json:"balance"`
	UID     string `json:"uid"`
}

type UserRequest struct {
	UID string `json:"uid"`
}

type VoucherRequest struct {
	UID       string `json:"uid"`
	MachineID string `json:"machine_id"`
}

type PrivilegeRequest struct {
	UID       string `json:"uid"`
	MachineID string `json:"machine_id"`
	FreeVend  bool   `json:"free_vend"`
}

var db *gorm.DB

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
        GROUP BY product, machine_id, is_cash, payment_method
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

func initDB() error {
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDB := os.Getenv("PG_DBNAME")
	pgHost := os.Getenv("PG_HOST")

	if pgUser == "" || pgPassword == "" || pgDB == "" || pgHost == "" {
		return fmt.Errorf("Missing one or more PostgreSQL environment variables")
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", pgHost, pgUser, pgPassword, pgDB)

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}

	// Auto-migrate the schema
	err = db.AutoMigrate(
		&User{},
		&UserMachinePrivilege{},
		&VendVoucher{},
		&TransactionModel{},
		&APIKey{},
		&ProductMap{},
	)
	if err != nil {
		return err
	}

	// Create indexes manually (GORM doesn't handle composite indexes in AutoMigrate well)
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_uid ON transactions(uid)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_metrics ON transactions(status, amount, product, machine_id, is_cash, payment_method)")

	return nil
}

func apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing API key", http.StatusUnauthorized)
			return
		}

		var key APIKey
		err := db.Where("key = ?", apiKey).First(&key).Error
		if err != nil {
			http.Error(w, "Invalid API key", http.StatusForbidden)
			return
		}

		// Check if the requested path is allowed
		requestedPath := r.URL.Path
		allowed := false
		for _, endpoint := range strings.Split(key.AllowedEndpoints, ",") {
			if strings.TrimSpace(endpoint) == requestedPath {
				allowed = true
				break
			}
		}

		if !allowed {
			http.Error(w, "API key not authorized for this endpoint", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func ensureUser(uid string) error {
	user := User{UID: uid}
	// FirstOrCreate will insert if not exists
	return db.Where(User{UID: uid}).FirstOrCreate(&user).Error
}

func topUpHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TopUpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.UID == "" || req.Amount <= 0 {
		http.Error(w, "Invalid top-up data", http.StatusBadRequest)
		return
	}

	if err := ensureUser(req.UID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transaction := TransactionModel{
		UID:    &req.UID,
		Amount: req.Amount,
		Status: "confirmed",
		IsCash: false,
	}

	if err := db.Create(&transaction).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintln(w, "Top-up successful")
}

func cashPurchaseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var purchase CashPurchase
	if err := json.NewDecoder(r.Body).Decode(&purchase); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if purchase.Amount <= 0 || purchase.Product <= 0 || purchase.MachineID == "" {
		http.Error(w, "Invalid purchase data", http.StatusBadRequest)
		return
	}

	transaction := TransactionModel{
		Amount:    -purchase.Amount,
		Status:    "confirmed",
		Product:   get_product_name(purchase.Product),
		MachineID: purchase.MachineID,
		IsCash:    true,
	}

	if err := db.Create(&transaction).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{"transaction_id": transaction.ID})
}

func get_product_name(id int) string {
	var product ProductMap
	err := db.Where("id = ?", id).First(&product).Error
	if err == nil {
		return product.ProductName
	}
	return fmt.Sprintf("%d", id)
}

func makePurchaseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PurchaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Product <= 0 || req.MachineID == "" {
		http.Error(w, "Missing fields", http.StatusBadRequest)
		return
	}

	useVoucher := false
	if req.UID != nil {
		if err := ensureUser(*req.UID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Check for free vend privilege
		var privilege UserMachinePrivilege
		err := db.Where("uid = ? AND machine_id = ?", *req.UID, req.MachineID).First(&privilege).Error
		if err == nil && privilege.FreeVend {
			req.Amount = 0
		} else {
			// Check for unused vouchers
			var voucher VendVoucher
			err := db.Where("uid = ? AND machine_id = ? AND used = ?", *req.UID, req.MachineID, false).First(&voucher).Error
			if err == nil {
				req.Amount = 0
				useVoucher = true
			}
		}

		// Check balance
		var balanceResult struct {
			Balance int
		}
		db.Model(&TransactionModel{}).
			Select("COALESCE(SUM(amount), 0) as balance").
			Where("uid = ? AND status = ?", *req.UID, "confirmed").
			Scan(&balanceResult)

		if balanceResult.Balance < req.Amount {
			http.Error(w, "Insufficient balance", http.StatusForbidden)
			return
		}
	}

	// Create pending transaction
	transaction := TransactionModel{
		UID:           req.UID,
		Amount:        -req.Amount,
		Product:       get_product_name(req.Product),
		Status:        "pending",
		PaymentMethod: ternary(req.UID == nil, "cash", "digital"),
		MachineID:     req.MachineID,
	}

	if err := db.Create(&transaction).Error; err != nil {
		http.Error(w, "Insert failed", http.StatusInternalServerError)
		return
	}

	// Mark voucher as used if applicable
	if useVoucher {
		db.Model(&VendVoucher{}).
			Where("uid = ? AND machine_id = ? AND used = ?", *req.UID, req.MachineID, false).
			Order("id ASC").
			Limit(1).
			Update("used", true)
	}

	// Set timeout for pending transactions
	go func(txID uint) {
		time.Sleep(60 * time.Second)
		db.Model(&TransactionModel{}).
			Where("id = ? AND status = ?", txID, "pending").
			Update("status", "failed")
	}(transaction.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"transaction_id": transaction.ID})
}

func confirmPurchaseHandler(w http.ResponseWriter, r *http.Request) {
	var req ConfirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result := db.Model(&TransactionModel{}).
		Where("id = ? AND status = ?", req.TransactionID, "pending").
		Update("status", "confirmed")

	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	if result.RowsAffected == 0 {
		http.Error(w, "Transaction not found or already processed", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getBalanceHandler(w http.ResponseWriter, r *http.Request) {
	var req BalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var balanceResult struct {
		Balance int
	}
	db.Model(&TransactionModel{}).
		Select("COALESCE(SUM(amount), 0) as balance").
		Where("uid = ? AND status = ?", req.UID, "confirmed").
		Scan(&balanceResult)

	json.NewEncoder(w).Encode(Balance{UID: req.UID, Balance: balanceResult.Balance})
}

func createUserHandler(w http.ResponseWriter, r *http.Request) {
	var req UserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	err := ensureUser(req.UID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func createVoucherHandler(w http.ResponseWriter, r *http.Request) {
	var req VoucherRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.MachineID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	err := ensureUser(req.UID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	voucher := VendVoucher{
		UID:       req.UID,
		MachineID: req.MachineID,
	}

	if err := db.Create(&voucher).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func createPrivilegeHandler(w http.ResponseWriter, r *http.Request) {
	var req PrivilegeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.MachineID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	err := ensureUser(req.UID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	privilege := UserMachinePrivilege{
		UID:       req.UID,
		MachineID: req.MachineID,
		FreeVend:  req.FreeVend,
	}

	// Use Clauses to handle ON CONFLICT
	if err := db.Save(&privilege).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

		// Handle preflight OPTIONS request
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Continue to the next handler
		next.ServeHTTP(w, r)
	})
}

// Prometheus Remote Read handler - allows querying historical metrics from the database
func remoteReadHandler(w http.ResponseWriter, r *http.Request) {
	req, err := remote.DecodeReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, query := range req.Queries {
		resp.Results[i] = executeRemoteReadQuery(query)
	}

	if err := remote.EncodeReadResponse(resp, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Execute a single remote read query against the database
func executeRemoteReadQuery(query *prompb.Query) *prompb.QueryResult {
	result := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{},
	}

	// Extract time range (convert milliseconds to seconds for SQL)
	startMs := query.StartTimestampMs
	endMs := query.EndTimestampMs

	// Parse matchers to extract label filters
	productFilter := ""
	machineFilter := ""
	methodFilter := ""
	matchesMetricName := false

	for _, matcher := range query.Matchers {
		if matcher.Name == "__name__" {
			// Check if this query is for our historical metric
			if matcher.Type == prompb.LabelMatcher_EQ && matcher.Value == "purchases_historical_total" {
				matchesMetricName = true
			} else if matcher.Type == prompb.LabelMatcher_RE && strings.Contains(matcher.Value, "purchases_historical_total") {
				matchesMetricName = true
			}
		} else if matcher.Name == "product" && matcher.Type == prompb.LabelMatcher_EQ {
			productFilter = matcher.Value
		} else if matcher.Name == "machine_id" && matcher.Type == prompb.LabelMatcher_EQ {
			machineFilter = matcher.Value
		} else if matcher.Name == "method" && matcher.Type == prompb.LabelMatcher_EQ {
			methodFilter = matcher.Value
		}
	}

	// Only process if this query is for our metric
	if !matchesMetricName {
		return result
	}

	// Build SQL query with optional filters
	// This query returns cumulative counts over time for each product/machine/method combination
	sqlQuery := `
		SELECT
			COALESCE(product, '') as product,
			COALESCE(machine_id, '') as machine_id,
			CASE
				WHEN is_cash = true THEN 'cash'
				ELSE COALESCE(payment_method, 'unknown')
			END as method,
			created_at,
			COUNT(*) OVER (
				PARTITION BY product, machine_id, is_cash, payment_method
				ORDER BY created_at
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) as cumulative_count
		FROM transactions
		WHERE status = 'confirmed'
		AND amount < 0
		AND EXTRACT(EPOCH FROM created_at) * 1000 BETWEEN ? AND ?
	`

	args := []interface{}{startMs, endMs}

	if productFilter != "" {
		sqlQuery += " AND product = ?"
		args = append(args, productFilter)
	}
	if machineFilter != "" {
		sqlQuery += " AND machine_id = ?"
		args = append(args, machineFilter)
	}
	if methodFilter != "" {
		if methodFilter == "cash" {
			sqlQuery += " AND is_cash = true"
		} else {
			sqlQuery += " AND is_cash = false AND payment_method = ?"
			args = append(args, methodFilter)
		}
	}

	sqlQuery += " ORDER BY product, machine_id, is_cash, payment_method, created_at"

	type RemoteReadRow struct {
		Product         string
		MachineID       string
		Method          string
		CreatedAt       time.Time
		CumulativeCount int64
	}

	var rows []RemoteReadRow
	err := db.Raw(sqlQuery, args...).Scan(&rows).Error
	if err != nil {
		log.Printf("Error querying remote read data: %v", err)
		return result
	}

	// Group samples by time series (product, machine_id, method)
	timeSeriesMap := make(map[string]*prompb.TimeSeries)

	for _, row := range rows {
		// Create time series key
		tsKey := fmt.Sprintf("%s|%s|%s", row.Product, row.MachineID, row.Method)

		// Get or create time series
		ts, exists := timeSeriesMap[tsKey]
		if !exists {
			ts = &prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "purchases_historical_total"},
					{Name: "product", Value: row.Product},
					{Name: "machine_id", Value: row.MachineID},
					{Name: "method", Value: row.Method},
				},
				Samples: []prompb.Sample{},
			}
			timeSeriesMap[tsKey] = ts
		}

		// Add sample (cumulative count at this timestamp)
		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: row.CreatedAt.UnixMilli(),
			Value:     float64(row.CumulativeCount),
		})
	}

	// Convert map to slice
	for _, ts := range timeSeriesMap {
		result.Timeseries = append(result.Timeseries, ts)
	}

	return result
}

func main() {
	if err := initDB(); err != nil {
		log.Fatal(err)
	}

	// Get underlying SQL DB for connection management
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/makePurchase", apiKeyMiddleware(makePurchaseHandler))
	mux.HandleFunc("/confirmPurchase", apiKeyMiddleware(confirmPurchaseHandler))
	mux.HandleFunc("/getBalance", apiKeyMiddleware(getBalanceHandler))
	mux.HandleFunc("/createUser", apiKeyMiddleware(createUserHandler))
	mux.HandleFunc("/createVoucher", apiKeyMiddleware(createVoucherHandler))
	mux.HandleFunc("/createPrivilege", apiKeyMiddleware(createPrivilegeHandler))
	mux.HandleFunc("/makeCashPurchase", apiKeyMiddleware(cashPurchaseHandler))
	mux.HandleFunc("/topUp", apiKeyMiddleware(topUpHandler))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/api/v1/read", remoteReadHandler) // Prometheus Remote Read endpoint (no auth required)

	handler := corsMiddleware(mux)

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
