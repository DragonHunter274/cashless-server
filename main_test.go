package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	embeddedPG     *embeddedpostgres.EmbeddedPostgres
	testAPIKey     = "test-api-key-12345"
	pgStarted      = false
	testDBInitOnce bool
)

// setupTestEnvironment creates an embedded PostgreSQL instance and initializes the application
func setupTestEnvironment(t *testing.T) *http.ServeMux {
	// Start embedded PostgreSQL only once
	if !pgStarted {
		embeddedPG = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Port(5433).
			Database("testdb").
			Username("postgres").
			Password("postgres"))

		if err := embeddedPG.Start(); err != nil {
			t.Fatalf("Failed to start embedded PostgreSQL: %v", err)
		}
		pgStarted = true

		// Set environment variables for the application
		// Note: GORM's PostgreSQL driver supports "host:port" format in the host field
		os.Setenv("PG_USER", "postgres")
		os.Setenv("PG_PASSWORD", "postgres")
		os.Setenv("PG_DBNAME", "testdb")
		os.Setenv("PG_HOST", "localhost port=5433")

		// Wait a moment for PostgreSQL to be fully ready
		time.Sleep(2 * time.Second)
	}

	// Initialize database connection using GORM (or reuse if already initialized)
	if db == nil {
		err := initDB()
		if err != nil {
			t.Fatalf("Failed to initialize database: %v", err)
		}
	}

	// Clear existing data for clean tests
	db.Exec("DELETE FROM transactions")
	db.Exec("DELETE FROM users")
	db.Exec("DELETE FROM vend_vouchers")
	db.Exec("DELETE FROM user_machine_privileges")
	db.Exec("DELETE FROM api_keys")

	// Create test API key with all permissions
	apiKey := APIKey{
		Key:              testAPIKey,
		AllowedEndpoints: "/makePurchase,/confirmPurchase,/makeCashPurchase,/getBalance,/getTransactions,/getVouchers,/getPrivileges,/topUp,/createUser,/createVoucher,/createPrivilege",
	}
	db.Create(&apiKey)

	// Initialize router matching main.go structure
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
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}

// teardownTestEnvironment cleans up the test database
func teardownTestEnvironment(t *testing.T) {
	// Don't close the database connection - we're reusing it across tests
	// The connection will be closed when all tests finish via TestMain cleanup
}

// TestMain runs before and after all tests
func TestMain(m *testing.M) {
	// Run all tests
	code := m.Run()

	// Cleanup after all tests
	if db != nil {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}
	if embeddedPG != nil {
		embeddedPG.Stop()
	}

	os.Exit(code)
}

// makeRequest is a helper function to make HTTP requests with the test API key
func makeRequest(t *testing.T, mux *http.ServeMux, method, path string, body interface{}, includeAPIKey bool) *httptest.ResponseRecorder {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("Failed to marshal request body: %v", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	if includeAPIKey {
		req.Header.Set("X-API-Key", testAPIKey)
	}

	recorder := httptest.NewRecorder()

	// Apply CORS middleware wrapper
	handler := corsMiddleware(mux)
	handler.ServeHTTP(recorder, req)

	return recorder
}

func TestCreateUser(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	reqBody := map[string]interface{}{
		"uid": "test-user-001",
	}

	resp := makeRequest(t, mux, "POST", "/createUser", reqBody, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify the user was actually created in the database
	var user User
	err := db.Where("uid = ?", "test-user-001").First(&user).Error
	if err != nil {
		t.Errorf("User was not created in database: %v", err)
	}
}

func TestGetBalance(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create a user first
	ensureUser("test-user-balance")

	reqBody := map[string]interface{}{
		"uid": "test-user-balance",
	}

	resp := makeRequest(t, mux, "POST", "/getBalance", reqBody, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if result["uid"] != "test-user-balance" {
		t.Errorf("Expected uid 'test-user-balance', got %v", result["uid"])
	}

	if result["balance"] != float64(0) {
		t.Errorf("Expected balance 0, got %v", result["balance"])
	}
}

func TestTopUp(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-topup"
	ensureUser(uid)

	reqBody := map[string]interface{}{
		"uid":    uid,
		"amount": 10050, // Amount in cents
	}

	resp := makeRequest(t, mux, "POST", "/topUp", reqBody, true)

	// topUpHandler returns 201 Created with plain text "Top-up successful"
	if resp.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify balance via getBalance endpoint (which returns JSON)
	balanceResp := makeRequest(t, mux, "POST", "/getBalance", map[string]interface{}{"uid": uid}, true)
	var balanceResult map[string]interface{}
	if err := json.Unmarshal(balanceResp.Body.Bytes(), &balanceResult); err != nil {
		t.Fatalf("Failed to parse balance response: %v", err)
	}

	if balanceResult["balance"] != float64(10050) {
		t.Errorf("Expected balance 10050 after top-up, got %v", balanceResult["balance"])
	}
}

func TestMakePurchaseAndConfirm(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-purchase"
	ensureUser(uid)

	// Top up the user first
	makeRequest(t, mux, "POST", "/topUp", map[string]interface{}{
		"uid":    uid,
		"amount": 5000, // 50.00 in cents
	}, true)

	// Make a purchase
	purchaseReq := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-001",
		"product":    123,
		"amount":     1575, // 15.75 in cents
	}

	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

	if purchaseResp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", purchaseResp.Code, purchaseResp.Body.String())
		return
	}

	// makePurchaseHandler returns JSON with transaction_id only (no status field)
	var purchaseResult map[string]interface{}
	if err := json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult); err != nil {
		t.Fatalf("Failed to parse purchase response: %v. Body: %s", err, purchaseResp.Body.String())
	}

	transactionID := int(purchaseResult["transaction_id"].(float64))
	if transactionID <= 0 {
		t.Fatalf("Expected valid transaction ID, got %d", transactionID)
	}

	// Verify transaction is pending in database
	var tx TransactionModel
	db.First(&tx, transactionID)
	if tx.Status != "pending" {
		t.Errorf("Expected status 'pending', got %s", tx.Status)
	}

	// Confirm the purchase
	confirmReq := map[string]interface{}{
		"transaction_id": transactionID,
	}

	confirmResp := makeRequest(t, mux, "POST", "/confirmPurchase", confirmReq, true)

	// confirmPurchaseHandler returns 200 with no body
	if confirmResp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", confirmResp.Code, confirmResp.Body.String())
		return
	}

	// Verify transaction status changed to confirmed in database
	db.First(&tx, transactionID)
	if tx.Status != "confirmed" {
		t.Errorf("Expected status 'confirmed', got %s", tx.Status)
	}

	// Verify final balance
	balanceResp := makeRequest(t, mux, "POST", "/getBalance", map[string]interface{}{"uid": uid}, true)
	var balanceResult map[string]interface{}
	json.Unmarshal(balanceResp.Body.Bytes(), &balanceResult)

	expectedBalance := 5000 - 1575
	if balanceResult["balance"] != float64(expectedBalance) {
		t.Errorf("Expected balance %d after purchase, got %v", expectedBalance, balanceResult["balance"])
	}
}

func TestMakePurchaseInsufficientFunds(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-insufficient"
	ensureUser(uid)

	// Try to make a purchase without funds
	purchaseReq := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-001",
		"product":    456,
		"amount":     2500,
	}

	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

	// makePurchaseHandler returns 403 Forbidden for insufficient balance
	if purchaseResp.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d. Body: %s", purchaseResp.Code, purchaseResp.Body.String())
	}

	if !bytes.Contains(purchaseResp.Body.Bytes(), []byte("Insufficient balance")) {
		t.Errorf("Expected error message about insufficient balance, got: %s", purchaseResp.Body.String())
	}
}

func TestMakeCashPurchase(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	reqBody := map[string]interface{}{
		"machine_id": "MACHINE-CASH-001",
		"product":    123,
		"amount":     550,
	}

	resp := makeRequest(t, mux, "POST", "/makeCashPurchase", reqBody, true)

	// cashPurchaseHandler returns 201 Created
	if resp.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Response contains only transaction_id
	var result map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v. Body: %s", err, resp.Body.String())
	}

	transactionID := int(result["transaction_id"].(float64))
	if transactionID <= 0 {
		t.Fatalf("Expected valid transaction ID, got %d", transactionID)
	}

	// Verify in database that it's a cash purchase and confirmed
	var tx TransactionModel
	db.First(&tx, transactionID)

	if !tx.IsCash {
		t.Errorf("Expected is_cash to be true")
	}

	if tx.Status != "confirmed" {
		t.Errorf("Expected status 'confirmed', got %s", tx.Status)
	}
}

func TestCreateVoucher(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-voucher"
	ensureUser(uid)

	reqBody := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-VOUCHER-001",
	}

	resp := makeRequest(t, mux, "POST", "/createVoucher", reqBody, true)

	// createVoucherHandler returns 200 with no body
	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify voucher was created in database
	var voucher VendVoucher
	err := db.Where("uid = ? AND machine_id = ?", uid, "MACHINE-VOUCHER-001").First(&voucher).Error
	if err != nil {
		t.Errorf("Voucher was not created in database: %v", err)
	}

	if voucher.Used {
		t.Errorf("Expected new voucher to be unused")
	}
}

func TestVoucherUsage(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-voucher-use"
	ensureUser(uid)

	// Create a voucher
	makeRequest(t, mux, "POST", "/createVoucher", map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-V-001",
	}, true)

	// Make a purchase using the voucher (should be free - amount set to 0)
	purchaseReq := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-V-001",
		"product":    123,
		"amount":     1000, // This will be overridden to 0 by voucher logic
	}

	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

	if purchaseResp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", purchaseResp.Code, purchaseResp.Body.String())
		return
	}

	var purchaseResult map[string]interface{}
	if err := json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult); err != nil {
		t.Fatalf("Failed to parse purchase response: %v. Body: %s", err, purchaseResp.Body.String())
	}

	transactionID := int(purchaseResult["transaction_id"].(float64))

	// Verify transaction is pending and amount is 0 (free vend)
	var tx TransactionModel
	db.First(&tx, transactionID)
	if tx.Status != "pending" {
		t.Errorf("Expected status 'pending', got %s", tx.Status)
	}
	if tx.Amount != 0 {
		t.Errorf("Expected amount 0 for voucher purchase, got %d", tx.Amount)
	}

	// Confirm the purchase
	confirmResp := makeRequest(t, mux, "POST", "/confirmPurchase", map[string]interface{}{
		"transaction_id": transactionID,
	}, true)

	if confirmResp.Code != http.StatusOK {
		t.Errorf("Expected confirm status 200, got %d", confirmResp.Code)
		return
	}

	// Balance should still be 0 (free vend)
	balanceResp := makeRequest(t, mux, "POST", "/getBalance", map[string]interface{}{"uid": uid}, true)
	var balanceResult map[string]interface{}
	json.Unmarshal(balanceResp.Body.Bytes(), &balanceResult)

	if balanceResult["balance"] != float64(0) {
		t.Errorf("Expected balance 0 after voucher use, got %v", balanceResult["balance"])
	}

	// Verify voucher was marked as used
	var voucher VendVoucher
	db.Where("uid = ? AND machine_id = ?", uid, "MACHINE-V-001").First(&voucher)
	if !voucher.Used {
		t.Errorf("Expected voucher to be marked as used")
	}
}

func TestCreatePrivilege(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-privilege"
	ensureUser(uid)

	reqBody := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-PRIV-001",
		"free_vend":  true,
	}

	resp := makeRequest(t, mux, "POST", "/createPrivilege", reqBody, true)

	// createPrivilegeHandler returns 200 with no body
	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify privilege was created in database
	var privilege UserMachinePrivilege
	err := db.Where("uid = ? AND machine_id = ?", uid, "MACHINE-PRIV-001").First(&privilege).Error
	if err != nil {
		t.Errorf("Privilege was not created in database: %v", err)
	}

	if !privilege.FreeVend {
		t.Errorf("Expected free_vend to be true")
	}
}

func TestPrivilegeUsage(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-privilege-use"
	ensureUser(uid)

	// Create a privilege
	makeRequest(t, mux, "POST", "/createPrivilege", map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-P-001",
		"free_vend":  true,
	}, true)

	// Make multiple purchases using the privilege (should all be free)
	for i := 0; i < 3; i++ {
		purchaseReq := map[string]interface{}{
			"uid":        uid,
			"machine_id": "MACHINE-P-001",
			"product":    100 + i,
			"amount":     2000,
		}

		purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

		if purchaseResp.Code != http.StatusOK {
			t.Errorf("Purchase %d: Expected status 200, got %d", i, purchaseResp.Code)
			continue
		}

		var purchaseResult map[string]interface{}
		json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult)

		// Confirm the purchase
		transactionID := int(purchaseResult["transaction_id"].(float64))
		makeRequest(t, mux, "POST", "/confirmPurchase", map[string]interface{}{
			"transaction_id": transactionID,
		}, true)
	}

	// Balance should still be 0 (all purchases were free)
	balanceResp := makeRequest(t, mux, "POST", "/getBalance", map[string]interface{}{"uid": uid}, true)
	var balanceResult map[string]interface{}
	json.Unmarshal(balanceResp.Body.Bytes(), &balanceResult)

	if balanceResult["balance"] != float64(0) {
		t.Errorf("Expected balance 0 after privilege uses, got %v", balanceResult["balance"])
	}
}

func TestPendingTransactionTimeout(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	uid := "test-user-timeout"
	ensureUser(uid)

	// Top up the user
	makeRequest(t, mux, "POST", "/topUp", map[string]interface{}{
		"uid":    uid,
		"amount": 10000,
	}, true)

	// Make a purchase but don't confirm it
	purchaseReq := map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-TIMEOUT-001",
		"product":    123,
		"amount":     1000,
	}

	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

	if purchaseResp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", purchaseResp.Code, purchaseResp.Body.String())
		return
	}

	var purchaseResult map[string]interface{}
	if err := json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult); err != nil {
		t.Fatalf("Failed to parse purchase response: %v. Body: %s", err, purchaseResp.Body.String())
	}

	transactionID := int(purchaseResult["transaction_id"].(float64))

	// Wait briefly and verify the transaction is pending
	time.Sleep(1 * time.Second)

	var tx TransactionModel
	err := db.First(&tx, transactionID).Error
	if err != nil {
		t.Fatalf("Failed to query transaction status: %v", err)
	}

	if tx.Status != "pending" {
		t.Errorf("Expected status 'pending' immediately after creation, got %s", tx.Status)
	}

	// The balance query only includes confirmed transactions, not pending ones
	// So balance should still be 10000 (the pending transaction doesn't affect it)
	balanceResp := makeRequest(t, mux, "POST", "/getBalance", map[string]interface{}{"uid": uid}, true)
	var balanceResult map[string]interface{}
	json.Unmarshal(balanceResp.Body.Bytes(), &balanceResult)

	// Balance only includes confirmed transactions
	if balanceResult["balance"] != float64(10000) {
		t.Errorf("Expected balance 10000 (pending not included), got %v", balanceResult["balance"])
	}
}

func TestMetricsEndpoint(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create some transactions first
	uid := "test-user-metrics"
	ensureUser(uid)
	makeRequest(t, mux, "POST", "/topUp", map[string]interface{}{
		"uid":    uid,
		"amount": 10000,
	}, true)

	// Make a confirmed purchase
	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", map[string]interface{}{
		"uid":        uid,
		"machine_id": "MACHINE-METRICS-001",
		"product":    123,
		"amount":     500,
	}, true)

	var purchaseResult map[string]interface{}
	json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult)
	transactionID := int(purchaseResult["transaction_id"].(float64))

	makeRequest(t, mux, "POST", "/confirmPurchase", map[string]interface{}{
		"transaction_id": transactionID,
	}, true)

	// Make a cash purchase
	makeRequest(t, mux, "POST", "/makeCashPurchase", map[string]interface{}{
		"machine_id": "MACHINE-METRICS-002",
		"product":    456,
		"amount":     350,
	}, true)

	// Request metrics (no API key required)
	resp := makeRequest(t, mux, "GET", "/metrics", nil, false)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
	}

	metricsBody := resp.Body.String()

	// Check for expected metric patterns
	if !bytes.Contains([]byte(metricsBody), []byte("promhttp_metric")) {
		t.Error("Expected Prometheus metrics in response")
	}
}

func TestAPIKeyRequired(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Try to access an endpoint without API key
	reqBody := map[string]interface{}{
		"uid": "test-user-noauth",
	}

	resp := makeRequest(t, mux, "POST", "/createUser", reqBody, false)

	if resp.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401 without API key, got %d", resp.Code)
	}
}

func TestAPIKeyInvalidEndpoint(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create an API key with limited permissions
	limitedAPIKey := "limited-key-12345"
	apiKey := APIKey{
		Key:              limitedAPIKey,
		AllowedEndpoints: "/getBalance",
	}
	db.Create(&apiKey)

	// Try to access a disallowed endpoint
	req := httptest.NewRequest("POST", "/createUser", bytes.NewBuffer([]byte(`{"uid":"test"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", limitedAPIKey)

	recorder := httptest.NewRecorder()
	handler := corsMiddleware(mux)
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Errorf("Expected status 403 for disallowed endpoint, got %d", recorder.Code)
	}
}

func TestCORSHeaders(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Make an OPTIONS request
	resp := makeRequest(t, mux, "OPTIONS", "/getBalance", nil, false)

	if resp.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("Expected CORS header 'Access-Control-Allow-Origin: *', got %s", resp.Header().Get("Access-Control-Allow-Origin"))
	}

	if resp.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("Expected CORS methods header to be set")
	}

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200 for OPTIONS request, got %d", resp.Code)
	}
}

func TestGetTransactions(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create a user
	ensureUser("test-transactions-user")

	// Create some test transactions
	// Top up
	topupReq := map[string]interface{}{
		"uid":    "test-transactions-user",
		"amount": 500,
	}
	makeRequest(t, mux, "POST", "/topUp", topupReq, true)

	// Make a purchase
	purchaseReq := map[string]interface{}{
		"uid":        "test-transactions-user",
		"amount":     100,
		"product":    1,
		"machine_id": "VM001",
	}
	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", purchaseReq, true)

	var purchaseResult map[string]interface{}
	json.Unmarshal(purchaseResp.Body.Bytes(), &purchaseResult)
	txID := int(purchaseResult["transaction_id"].(float64))

	// Confirm the purchase
	confirmReq := map[string]interface{}{
		"transaction_id": txID,
	}
	makeRequest(t, mux, "POST", "/confirmPurchase", confirmReq, true)

	// Make a cash purchase
	cashReq := map[string]interface{}{
		"amount":     150,
		"product":    2,
		"machine_id": "VM002",
	}
	makeRequest(t, mux, "POST", "/makeCashPurchase", cashReq, true)

	// Now test getting all transactions
	getAllReq := map[string]interface{}{
		"limit": 100,
	}
	resp := makeRequest(t, mux, "POST", "/getTransactions", getAllReq, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var transactions []map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &transactions); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Should have at least 3 transactions (topup, purchase, cash purchase)
	if len(transactions) < 3 {
		t.Errorf("Expected at least 3 transactions, got %d", len(transactions))
	}

	// Test filtering by UID
	filterReq := map[string]interface{}{
		"uid":   "test-transactions-user",
		"limit": 100,
	}
	resp = makeRequest(t, mux, "POST", "/getTransactions", filterReq, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var filteredTransactions []map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &filteredTransactions); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Should have 2 transactions for this user (topup and purchase, not cash)
	if len(filteredTransactions) != 2 {
		t.Errorf("Expected 2 transactions for user, got %d", len(filteredTransactions))
	}

	// Verify all filtered transactions belong to the user
	for _, tx := range filteredTransactions {
		if tx["uid"] != "test-transactions-user" {
			t.Errorf("Expected all transactions to have uid 'test-transactions-user', got %v", tx["uid"])
		}
	}

	// Test pagination with limit
	limitReq := map[string]interface{}{
		"limit": 1,
	}
	resp = makeRequest(t, mux, "POST", "/getTransactions", limitReq, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var limitedTransactions []map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &limitedTransactions); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if len(limitedTransactions) != 1 {
		t.Errorf("Expected 1 transaction with limit=1, got %d", len(limitedTransactions))
	}
}
