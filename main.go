package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ... (keep all your existing struct definitions)

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

var db *sql.DB

// Custom Prometheus collector that fetches data from database
type PurchaseCollector struct {
	purchaseDesc *prometheus.Desc
}

func NewPurchaseCollector() *PurchaseCollector {
	return &PurchaseCollector{
		purchaseDesc: prometheus.NewDesc(
			"purchases_total",
			"Total number of confirmed purchases",
			[]string{"product", "machine_id", "method"},
			nil,
		),
	}
}

func (c *PurchaseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.purchaseDesc
}

func (c *PurchaseCollector) Collect(ch chan<- prometheus.Metric) {
	if db == nil {
		return
	}

	// Query the database for confirmed purchases grouped by product, machine_id, and payment method
	rows, err := db.Query(`
		SELECT 
			COALESCE(product, '') as product,
			COALESCE(machine_id, '') as machine_id,
			CASE 
				WHEN is_cash = true THEN 'cash'
				ELSE COALESCE(payment_method, 'unknown')
			END as method,
			COUNT(*) as count
		FROM transactions 
		WHERE status = 'confirmed' 
			AND amount < 0  -- Only actual purchases (negative amounts)
		GROUP BY product, machine_id, is_cash, payment_method
	`)
	if err != nil {
		log.Printf("Error querying purchase metrics: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var product, machineID, method string
		var count float64

		if err := rows.Scan(&product, &machineID, &method, &count); err != nil {
			log.Printf("Error scanning purchase metric row: %v", err)
			continue
		}

		metric, err := prometheus.NewConstMetric(
			c.purchaseDesc,
			prometheus.CounterValue,
			count,
			product, machineID, method,
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
	var err error

	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDB := os.Getenv("PG_DBNAME")
	pgHost := os.Getenv("PG_HOST")

	if pgUser == "" || pgPassword == "" || pgDB == "" || pgHost == "" {
		return fmt.Errorf("Missing one or more PostgreSQL environment variables")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", pgUser, pgPassword, pgHost, pgDB)
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	tables := []string{
		`CREATE TABLE IF NOT EXISTS users (
		uid TEXT PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`,
		`CREATE TABLE IF NOT EXISTS user_machine_privileges (
		uid TEXT,
		machine_id TEXT,
		free_vend BOOLEAN DEFAULT FALSE,
		PRIMARY KEY (uid, machine_id),
		FOREIGN KEY (uid) REFERENCES users(uid)
	);`,
		`CREATE TABLE IF NOT EXISTS vend_vouchers (
		id SERIAL PRIMARY KEY,
		uid TEXT,
		machine_id TEXT,
		used BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (uid) REFERENCES users(uid)
	);`,
		`CREATE TABLE IF NOT EXISTS transactions (
		id SERIAL PRIMARY KEY,
		uid TEXT,
		amount INTEGER NOT NULL,
		product TEXT,
		status TEXT,
		payment_method TEXT,
		machine_id TEXT,
		is_cash BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`,
		`CREATE TABLE IF NOT EXISTS api_keys (
		key TEXT PRIMARY KEY,
		allowed_endpoints TEXT, -- comma-separated list of paths
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`,
		`CREATE TABLE IF NOT EXISTS product_map (
		id INT PRIMARY KEY,
		product_name TEXT
	);`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_uid ON transactions(uid);`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_metrics ON transactions(status, amount, product, machine_id, is_cash, payment_method);`,
	}

	for _, table := range tables {
		if _, err = db.Exec(table); err != nil {
			return err
		}
	}

	return nil
}

func apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing API key", http.StatusUnauthorized)
			return
		}

		var allowedEndpoints string
		err := db.QueryRow("SELECT allowed_endpoints FROM api_keys WHERE key = $1", apiKey).Scan(&allowedEndpoints)
		if err != nil {
			http.Error(w, "Invalid API key", http.StatusForbidden)
			return
		}

		// Check if the requested path is allowed
		requestedPath := r.URL.Path
		allowed := false
		for _, endpoint := range strings.Split(allowedEndpoints, ",") {
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
	_, err := db.Exec(`INSERT INTO users (uid) VALUES ($1) ON CONFLICT DO NOTHING`, uid)
	return err
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

	_, err := db.Exec(`
		INSERT INTO users (uid) VALUES ($1)
		ON CONFLICT (uid) DO NOTHING
	`, req.UID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = db.Exec(`
		INSERT INTO transactions (uid, amount, status, created_at, is_cash)
		VALUES ($1, $2, 'confirmed', NOW(), false)
	`, req.UID, req.Amount)
	if err != nil {
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

	var txID int64
	err := db.QueryRow(`
		INSERT INTO transactions (amount, status, product, machine_id, created_at, is_cash)
		VALUES ($1, 'confirmed', $2, $3, NOW(), true)
		RETURNING id
	`, -purchase.Amount, get_product_name(purchase.Product), purchase.MachineID).Scan(&txID)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{"transaction_id": txID})
}

func get_product_name(id int) string {
	var productName string
	err := db.QueryRow(`SELECT product_name FROM product_map WHERE id = $1`, id).Scan(&productName)
	if err == nil {
		return productName
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

		var freeVend bool
		err := db.QueryRow(`SELECT free_vend FROM user_machine_privileges WHERE uid = $1 AND machine_id = $2`, *req.UID, req.MachineID).Scan(&freeVend)
		if err == nil && freeVend {
			req.Amount = 0
		} else {
			var voucherID int
			err := db.QueryRow(`SELECT id FROM vend_vouchers WHERE uid = $1 AND machine_id = $2 AND used = FALSE LIMIT 1`, *req.UID, req.MachineID).Scan(&voucherID)
			if err == nil {
				req.Amount = 0
				useVoucher = true
			}
		}

		var balance int
		db.QueryRow(`SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE uid = $1 AND status = 'confirmed'`, *req.UID).Scan(&balance)
		if balance < req.Amount {
			http.Error(w, "Insufficient balance", http.StatusForbidden)
			return
		}
	}

	var txID int64
	err := db.QueryRow(`
		INSERT INTO transactions (uid, amount, product, status, payment_method, machine_id)
		VALUES ($1, $2, $3, 'pending', $4, $5)
		RETURNING id`,
		req.UID,
		-req.Amount,
		get_product_name(req.Product),
		ternary(req.UID == nil, "cash", "digital"),
		req.MachineID,
	).Scan(&txID)
	if err != nil {
		http.Error(w, "Insert failed", http.StatusInternalServerError)
		return
	}

	if useVoucher {
		//_, _ = db.Exec(`UPDATE vend_vouchers SET used = TRUE WHERE uid = $1 AND machine_id = $2 AND used = FALSE LIMIT 1`, *req.UID, req.MachineID)
		_, _ = db.Exec(`UPDATE vend_vouchers SET used = TRUE WHERE id = (SELECT id FROM vend_vouchers WHERE uid = $1 AND machine_id = $2 AND used = FALSE LIMIT 1)`, *req.UID, req.MachineID) //potential fix for voucher issue
	}

	// Set timeout for pending transactions
	go func(txID int64) {
		time.Sleep(60 * time.Second)
		db.Exec(`UPDATE transactions SET status = 'failed' WHERE id = $1 AND status = 'pending'`, txID)
	}(txID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"transaction_id": txID})
}

func confirmPurchaseHandler(w http.ResponseWriter, r *http.Request) {
	var req ConfirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := db.Exec(`UPDATE transactions SET status = 'confirmed' WHERE id = $1 AND status = 'pending'`, req.TransactionID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
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

	var balance int
	db.QueryRow(`SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE uid = $1 AND status = 'confirmed'`, req.UID).Scan(&balance)
	json.NewEncoder(w).Encode(Balance{UID: req.UID, Balance: balance})
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
	_, err = db.Exec(`INSERT INTO vend_vouchers (uid, machine_id) VALUES ($1, $2)`, req.UID, req.MachineID)
	if err != nil {
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
	_, err = db.Exec(`
		INSERT INTO user_machine_privileges (uid, machine_id, free_vend)
		VALUES ($1, $2, $3)
		ON CONFLICT (uid, machine_id) DO UPDATE SET free_vend = EXCLUDED.free_vend`, req.UID, req.MachineID, req.FreeVend)
	if err != nil {
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

func main() {
	if err := initDB(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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

	handler := corsMiddleware(mux)

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
