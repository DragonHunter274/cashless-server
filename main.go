package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
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

type TransactionsRequest struct {
	UID    string `json:"uid"`    // Optional: filter by user
	Limit  int    `json:"limit"`  // Optional: limit results (default 100)
	Offset int    `json:"offset"` // Optional: offset for pagination
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

type StatsResponse struct {
	TotalUsers            int64         `json:"total_users"`
	TotalRevenue          int64         `json:"total_revenue"`
	TotalTransactions     int64         `json:"total_transactions"`
	ConfirmedTransactions int64         `json:"confirmed_transactions"`
	PendingTransactions   int64         `json:"pending_transactions"`
	FailedTransactions    int64         `json:"failed_transactions"`
	ActiveVouchers        int64         `json:"active_vouchers"`
	UsedVouchers          int64         `json:"used_vouchers"`
	TotalPrivileges       int64         `json:"total_privileges"`
	RecentTransactions    []Transaction `json:"recent_transactions"`
}

type GetUsersRequest struct {
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Search string `json:"search"`
}

type UserWithBalance struct {
	UID       string    `json:"uid"`
	Balance   int       `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

type GetUsersResponse struct {
	Users []UserWithBalance `json:"users"`
	Total int64             `json:"total"`
}

type CreateAPIKeyRequest struct {
	AllowedEndpoints string `json:"allowed_endpoints"`
}

type DeleteAPIKeyRequest struct {
	Key string `json:"key"`
}

type ProductMapRequest struct {
	ID          int    `json:"id"`
	ProductName string `json:"product_name"`
}

type DeleteByIDRequest struct {
	ID uint `json:"id"`
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

func generateAPIKey() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		log.Fatal("Failed to generate API key:", err)
	}
	return hex.EncodeToString(bytes)
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

func setupTestMode() (*embeddedpostgres.EmbeddedPostgres, string, error) {
	log.Println("Starting in TEST MODE with embedded PostgreSQL...")

	// Start embedded PostgreSQL on port 5434 to avoid conflicts
	embeddedPG := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(5434).
		Database("cashless_test").
		Username("postgres").
		Password("postgres"))

	if err := embeddedPG.Start(); err != nil {
		return nil, "", fmt.Errorf("failed to start embedded PostgreSQL: %v", err)
	}

	// Set environment variables for database connection
	os.Setenv("PG_USER", "postgres")
	os.Setenv("PG_PASSWORD", "postgres")
	os.Setenv("PG_DBNAME", "cashless_test")
	os.Setenv("PG_HOST", "localhost port=5434")

	// Wait for PostgreSQL to be ready
	time.Sleep(2 * time.Second)

	// Initialize database
	if err := initDB(); err != nil {
		embeddedPG.Stop()
		return nil, "", fmt.Errorf("failed to initialize database: %v", err)
	}

	// Generate and create API key
	apiKey := generateAPIKey()
	key := APIKey{
		Key:              apiKey,
		AllowedEndpoints: "/makePurchase,/confirmPurchase,/makeCashPurchase,/getBalance,/getTransactions,/getVouchers,/getPrivileges,/topUp,/createUser,/createVoucher,/createPrivilege,/getStats,/getUsers,/getAPIKeys,/createAPIKey,/deleteAPIKey,/getProductMap,/createProductMapping,/deleteProductMapping,/deleteVoucher,/deletePrivilege",
	}
	if err := db.Create(&key).Error; err != nil {
		embeddedPG.Stop()
		return nil, "", fmt.Errorf("failed to create API key: %v", err)
	}

	return embeddedPG, apiKey, nil
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

func getTransactionsHandler(w http.ResponseWriter, r *http.Request) {
	var req TransactionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Set default limit if not provided
	if req.Limit <= 0 {
		req.Limit = 100
	}

	// Cap maximum limit at 1000
	if req.Limit > 1000 {
		req.Limit = 1000
	}

	query := db.Model(&TransactionModel{}).Order("created_at DESC")

	// Filter by UID if provided
	if req.UID != "" {
		query = query.Where("uid = ?", req.UID)
	}

	// Apply pagination
	query = query.Limit(req.Limit).Offset(req.Offset)

	var transactions []TransactionModel
	if err := query.Find(&transactions).Error; err != nil {
		http.Error(w, "Error fetching transactions: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to response format
	var response []Transaction
	for _, t := range transactions {
		response = append(response, Transaction{
			ID:        int(t.ID),
			UID:       t.UID,
			Amount:    t.Amount,
			Product:   t.Product,
			Status:    t.Status,
			Method:    t.PaymentMethod,
			MachineID: t.MachineID,
			CreatedAt: t.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func getVouchersHandler(w http.ResponseWriter, r *http.Request) {
	var req UserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	query := db.Model(&VendVoucher{})

	// Filter by UID if provided
	if req.UID != "" {
		query = query.Where("uid = ?", req.UID)
	}

	var vouchers []VendVoucher
	if err := query.Order("created_at DESC").Find(&vouchers).Error; err != nil {
		http.Error(w, "Error fetching vouchers: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(vouchers)
}

func getPrivilegesHandler(w http.ResponseWriter, r *http.Request) {
	var req UserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	query := db.Model(&UserMachinePrivilege{})

	// Filter by UID if provided
	if req.UID != "" {
		query = query.Where("uid = ?", req.UID)
	}

	var privileges []UserMachinePrivilege
	if err := query.Find(&privileges).Error; err != nil {
		http.Error(w, "Error fetching privileges: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(privileges)
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

func getStatsHandler(w http.ResponseWriter, r *http.Request) {
	var stats StatsResponse

	db.Model(&User{}).Count(&stats.TotalUsers)
	db.Model(&TransactionModel{}).Count(&stats.TotalTransactions)
	db.Model(&TransactionModel{}).Where("status = ?", "confirmed").Count(&stats.ConfirmedTransactions)
	db.Model(&TransactionModel{}).Where("status = ?", "pending").Count(&stats.PendingTransactions)
	db.Model(&TransactionModel{}).Where("status = ?", "failed").Count(&stats.FailedTransactions)
	db.Model(&VendVoucher{}).Where("used = ?", false).Count(&stats.ActiveVouchers)
	db.Model(&VendVoucher{}).Where("used = ?", true).Count(&stats.UsedVouchers)
	db.Model(&UserMachinePrivilege{}).Count(&stats.TotalPrivileges)

	var revenueResult struct{ Total int64 }
	db.Model(&TransactionModel{}).
		Select("COALESCE(SUM(ABS(amount)), 0) as total").
		Where("status = ? AND amount < 0", "confirmed").
		Scan(&revenueResult)
	stats.TotalRevenue = revenueResult.Total

	var recentTx []TransactionModel
	db.Order("created_at DESC").Limit(10).Find(&recentTx)
	for _, t := range recentTx {
		stats.RecentTransactions = append(stats.RecentTransactions, Transaction{
			ID:        int(t.ID),
			UID:       t.UID,
			Amount:    t.Amount,
			Product:   t.Product,
			Status:    t.Status,
			Method:    t.PaymentMethod,
			MachineID: t.MachineID,
			CreatedAt: t.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func getUsersHandler(w http.ResponseWriter, r *http.Request) {
	var req GetUsersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 50
	}
	if req.Limit > 500 {
		req.Limit = 500
	}

	query := db.Model(&User{})
	if req.Search != "" {
		query = query.Where("uid ILIKE ?", "%"+req.Search+"%")
	}

	var total int64
	query.Count(&total)

	var users []User
	query.Order("created_at DESC").Limit(req.Limit).Offset(req.Offset).Find(&users)

	var result []UserWithBalance
	for _, u := range users {
		var balanceResult struct{ Balance int }
		db.Model(&TransactionModel{}).
			Select("COALESCE(SUM(amount), 0) as balance").
			Where("uid = ? AND status = ?", u.UID, "confirmed").
			Scan(&balanceResult)

		result = append(result, UserWithBalance{
			UID:       u.UID,
			Balance:   balanceResult.Balance,
			CreatedAt: u.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(GetUsersResponse{Users: result, Total: total})
}

func getAPIKeysHandler(w http.ResponseWriter, r *http.Request) {
	var keys []APIKey
	db.Order("created_at DESC").Find(&keys)

	type MaskedKey struct {
		Key              string    `json:"key"`
		AllowedEndpoints string    `json:"allowed_endpoints"`
		CreatedAt        time.Time `json:"created_at"`
	}

	var masked []MaskedKey
	for _, k := range keys {
		maskedKey := k.Key
		if len(k.Key) > 8 {
			maskedKey = k.Key[:4] + "..." + k.Key[len(k.Key)-4:]
		}
		masked = append(masked, MaskedKey{
			Key:              maskedKey,
			AllowedEndpoints: k.AllowedEndpoints,
			CreatedAt:        k.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(masked)
}

func createAPIKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.AllowedEndpoints == "" {
		http.Error(w, "allowed_endpoints is required", http.StatusBadRequest)
		return
	}

	key := APIKey{
		Key:              generateAPIKey(),
		AllowedEndpoints: req.AllowedEndpoints,
	}

	if err := db.Create(&key).Error; err != nil {
		http.Error(w, "Failed to create API key", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(key)
}

func deleteAPIKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Prevent self-deletion
	currentKey := r.Header.Get("X-API-Key")
	if req.Key == currentKey {
		http.Error(w, "Cannot delete the API key currently in use", http.StatusBadRequest)
		return
	}

	result := db.Where("key = ?", req.Key).Delete(&APIKey{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "API key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getProductMapHandler(w http.ResponseWriter, r *http.Request) {
	var products []ProductMap
	db.Order("id ASC").Find(&products)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

func createProductMappingHandler(w http.ResponseWriter, r *http.Request) {
	var req ProductMapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.ID <= 0 || req.ProductName == "" {
		http.Error(w, "id and product_name are required", http.StatusBadRequest)
		return
	}

	product := ProductMap{ID: req.ID, ProductName: req.ProductName}
	if err := db.Save(&product).Error; err != nil {
		http.Error(w, "Failed to save product mapping", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func deleteProductMappingHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteByIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result := db.Where("id = ?", req.ID).Delete(&ProductMap{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "Product mapping not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func deleteVoucherHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteByIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var voucher VendVoucher
	if err := db.First(&voucher, req.ID).Error; err != nil {
		http.Error(w, "Voucher not found", http.StatusNotFound)
		return
	}

	if voucher.Used {
		http.Error(w, "Cannot delete used voucher", http.StatusBadRequest)
		return
	}

	db.Delete(&voucher)
	w.WriteHeader(http.StatusOK)
}

func deletePrivilegeHandler(w http.ResponseWriter, r *http.Request) {
	var req VoucherRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.MachineID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result := db.Where("uid = ? AND machine_id = ?", req.UID, req.MachineID).Delete(&UserMachinePrivilege{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "Privilege not found", http.StatusNotFound)
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

// Helper function to check if a time series matches label matchers
func matchesLabels(ts *prompb.TimeSeries, matchers []*prompb.LabelMatcher) bool {
	labelMap := make(map[string]string)
	for _, label := range ts.Labels {
		labelMap[label.Name] = label.Value
	}

	for _, matcher := range matchers {
		value, exists := labelMap[matcher.Name]
		if !exists {
			value = ""
		}

		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if value != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NEQ:
			if value == matcher.Value {
				return false
			}
		case prompb.LabelMatcher_RE:
			// Simple regex match for common patterns
			if !strings.Contains(value, matcher.Value) {
				return false
			}
		case prompb.LabelMatcher_NRE:
			if strings.Contains(value, matcher.Value) {
				return false
			}
		}
	}
	return true
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

	// Debug: Log all matchers received from Prometheus
	log.Printf("Remote read query - Matchers: %d total", len(query.Matchers))
	for i, matcher := range query.Matchers {
		matcherType := "UNKNOWN"
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			matcherType = "=="
		case prompb.LabelMatcher_NEQ:
			matcherType = "!="
		case prompb.LabelMatcher_RE:
			matcherType = "=~"
		case prompb.LabelMatcher_NRE:
			matcherType = "!~"
		}
		log.Printf("  Matcher %d: %s %s %q", i, matcher.Name, matcherType, matcher.Value)
	}

	// Only process if this query is for our metric
	if !matchesMetricName {
		log.Printf("Query does not match metric name, returning empty result")
		return result
	}

	// Build SQL query with optional filters
	// This query returns cumulative counts over time for each product/machine/method combination
	// We need to get ALL transactions up to endMs to calculate proper cumulative counts,
	// but we'll filter to the query range after getting the baseline
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
		AND EXTRACT(EPOCH FROM created_at) * 1000 <= ?
	`

	args := []interface{}{endMs}

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

	// Process samples and add interpolated points for proper counter visualization
	for _, ts := range timeSeriesMap {
		if len(ts.Samples) == 0 {
			continue
		}

		// Find the baseline value (count at startMs) and samples within range
		var baselineValue float64 = 0
		var filteredSamples []prompb.Sample

		for _, sample := range ts.Samples {
			if sample.Timestamp < startMs {
				// Track the counter value just before our query range
				baselineValue = sample.Value
			} else {
				// This sample is within our query range
				filteredSamples = append(filteredSamples, sample)
			}
		}

		// Skip time series with no transactions in the query range
		// This hides products that had no purchases during the selected time period
		if len(filteredSamples) == 0 {
			continue
		}

		// Calculate step interval for interpolation
		// Match Prometheus scrape interval (typically 1 minute) for short ranges,
		// but use larger intervals for longer ranges to avoid too many points
		rangeMs := endMs - startMs
		var stepMs int64

		if rangeMs > 30*24*60*60*1000 { // > 30 days
			stepMs = 60 * 60 * 1000 // 1 hour
		} else if rangeMs > 7*24*60*60*1000 { // > 7 days
			stepMs = 15 * 60 * 1000 // 15 minutes
		} else if rangeMs > 24*60*60*1000 { // > 1 day
			stepMs = 5 * 60 * 1000 // 5 minutes
		} else if rangeMs > 6*60*60*1000 { // > 6 hours
			stepMs = 2 * 60 * 1000 // 2 minutes
		} else {
			stepMs = 60 * 1000 // 1 minute for < 6 hours
		}

		// Build a merged list of interpolated points AND actual transaction times
		var allSampleTimes []int64
		timeMap := make(map[int64]bool)

		// Add regular interval times
		for t := startMs; t <= endMs; t += stepMs {
			allSampleTimes = append(allSampleTimes, t)
			timeMap[t] = true
		}

		// Add actual transaction times if not already present
		for _, sample := range filteredSamples {
			if !timeMap[sample.Timestamp] {
				allSampleTimes = append(allSampleTimes, sample.Timestamp)
				timeMap[sample.Timestamp] = true
			}
		}

		// Always include start and end
		if !timeMap[startMs] {
			allSampleTimes = append(allSampleTimes, startMs)
		}
		if !timeMap[endMs] {
			allSampleTimes = append(allSampleTimes, endMs)
		}

		// Sort all times
		sortInt64Slice(allSampleTimes)

		// Generate samples at all these times
		var interpolatedSamples []prompb.Sample
		currentValue := baselineValue
		sampleIdx := 0

		for _, timestamp := range allSampleTimes {
			// Update value based on any transactions up to this point
			for sampleIdx < len(filteredSamples) && filteredSamples[sampleIdx].Timestamp <= timestamp {
				currentValue = filteredSamples[sampleIdx].Value
				sampleIdx++
			}

			interpolatedSamples = append(interpolatedSamples, prompb.Sample{
				Timestamp: timestamp,
				Value:     currentValue,
			})
		}

		if len(interpolatedSamples) > 0 {
			ts.Samples = interpolatedSamples

			// Apply label matchers to filter time series
			if matchesLabels(ts, query.Matchers) {
				result.Timeseries = append(result.Timeseries, ts)
			} else {
				// Debug: Log why this time series was filtered out
				var productLabel string
				for _, label := range ts.Labels {
					if label.Name == "product" {
						productLabel = label.Value
						break
					}
				}
				log.Printf("  Filtered out time series: product=%s (didn't match matchers)", productLabel)
			}
		}
	}

	log.Printf("Returning %d time series after filtering", len(result.Timeseries))
	return result
}

// Helper function to sort int64 slices
func sortInt64Slice(slice []int64) {
	for i := 0; i < len(slice); i++ {
		for j := i + 1; j < len(slice); j++ {
			if slice[i] > slice[j] {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}
}

func main() {
	// Parse command-line flags
	testMode := flag.Bool("test", false, "Run in test mode with embedded PostgreSQL")
	flag.Parse()

	var embeddedPG *embeddedpostgres.EmbeddedPostgres
	var apiKey string

	if *testMode {
		var err error
		embeddedPG, apiKey, err = setupTestMode()
		if err != nil {
			log.Fatal(err)
		}

		// Display API key prominently
		log.Println("================================================================================")
		log.Println("TEST MODE ACTIVE - Embedded PostgreSQL running on port 5434")
		log.Println("================================================================================")
		log.Println("API Key for Web UI:")
		log.Println(apiKey)
		log.Println("================================================================================")
		log.Println("Copy the API key above and paste it into the web UI at http://localhost:8080")
		log.Println("================================================================================")
	} else {
		if err := initDB(); err != nil {
			log.Fatal(err)
		}
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
	mux.HandleFunc("/getTransactions", apiKeyMiddleware(getTransactionsHandler))
	mux.HandleFunc("/getVouchers", apiKeyMiddleware(getVouchersHandler))
	mux.HandleFunc("/getPrivileges", apiKeyMiddleware(getPrivilegesHandler))
	mux.HandleFunc("/createUser", apiKeyMiddleware(createUserHandler))
	mux.HandleFunc("/createVoucher", apiKeyMiddleware(createVoucherHandler))
	mux.HandleFunc("/createPrivilege", apiKeyMiddleware(createPrivilegeHandler))
	mux.HandleFunc("/makeCashPurchase", apiKeyMiddleware(cashPurchaseHandler))
	mux.HandleFunc("/topUp", apiKeyMiddleware(topUpHandler))
	mux.HandleFunc("/getStats", apiKeyMiddleware(getStatsHandler))
	mux.HandleFunc("/getUsers", apiKeyMiddleware(getUsersHandler))
	mux.HandleFunc("/getAPIKeys", apiKeyMiddleware(getAPIKeysHandler))
	mux.HandleFunc("/createAPIKey", apiKeyMiddleware(createAPIKeyHandler))
	mux.HandleFunc("/deleteAPIKey", apiKeyMiddleware(deleteAPIKeyHandler))
	mux.HandleFunc("/getProductMap", apiKeyMiddleware(getProductMapHandler))
	mux.HandleFunc("/createProductMapping", apiKeyMiddleware(createProductMappingHandler))
	mux.HandleFunc("/deleteProductMapping", apiKeyMiddleware(deleteProductMappingHandler))
	mux.HandleFunc("/deleteVoucher", apiKeyMiddleware(deleteVoucherHandler))
	mux.HandleFunc("/deletePrivilege", apiKeyMiddleware(deletePrivilegeHandler))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/api/v1/read", remoteReadHandler) // Prometheus Remote Read endpoint (no auth required)

	// Serve static files from the static directory
	fs := http.FileServer(http.Dir("./static"))
	mux.Handle("/", fs)

	handler := corsMiddleware(mux)

	// Create HTTP server with graceful shutdown support
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		log.Println("Server started on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Wait for interrupt signal
	<-sigChan
	log.Println("\nShutting down gracefully...")

	// Shutdown HTTP server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	// Stop embedded PostgreSQL if running in test mode
	if embeddedPG != nil {
		log.Println("Stopping embedded PostgreSQL...")
		if err := embeddedPG.Stop(); err != nil {
			log.Printf("Error stopping embedded PostgreSQL: %v", err)
		} else {
			log.Println("Embedded PostgreSQL stopped successfully")
		}
	}

	log.Println("Server stopped")
}
