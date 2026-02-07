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
	db.Exec("DELETE FROM product_maps")

	// Create test API key with all permissions
	apiKey := APIKey{
		Key:              testAPIKey,
		AllowedEndpoints: "/makePurchase,/confirmPurchase,/makeCashPurchase,/getBalance,/getTransactions,/getVouchers,/getPrivileges,/topUp,/createUser,/createVoucher,/createPrivilege,/getStats,/getUsers,/getAPIKeys,/createAPIKey,/deleteAPIKey,/getProductMap,/createProductMapping,/deleteProductMapping,/deleteVoucher,/deletePrivilege",
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

func TestGetStats(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create test data
	ensureUser("stats-user-1")
	ensureUser("stats-user-2")

	// Top up and make purchases
	makeRequest(t, mux, "POST", "/topUp", map[string]interface{}{"uid": "stats-user-1", "amount": 5000}, true)

	purchaseResp := makeRequest(t, mux, "POST", "/makePurchase", map[string]interface{}{
		"uid": "stats-user-1", "machine_id": "VM1", "product": 1, "amount": 300,
	}, true)
	var pr map[string]interface{}
	json.Unmarshal(purchaseResp.Body.Bytes(), &pr)
	makeRequest(t, mux, "POST", "/confirmPurchase", map[string]interface{}{"transaction_id": int(pr["transaction_id"].(float64))}, true)

	makeRequest(t, mux, "POST", "/makeCashPurchase", map[string]interface{}{
		"machine_id": "VM2", "product": 2, "amount": 200,
	}, true)

	// Create voucher and privilege
	makeRequest(t, mux, "POST", "/createVoucher", map[string]interface{}{"uid": "stats-user-1", "machine_id": "VM1"}, true)
	makeRequest(t, mux, "POST", "/createPrivilege", map[string]interface{}{"uid": "stats-user-1", "machine_id": "VM1", "free_vend": true}, true)

	resp := makeRequest(t, mux, "POST", "/getStats", map[string]interface{}{}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var stats StatsResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &stats); err != nil {
		t.Fatalf("Failed to parse stats: %v", err)
	}

	if stats.TotalUsers != 2 {
		t.Errorf("Expected 2 users, got %d", stats.TotalUsers)
	}
	if stats.ConfirmedTransactions < 3 {
		t.Errorf("Expected at least 3 confirmed transactions, got %d", stats.ConfirmedTransactions)
	}
	if stats.TotalRevenue != 500 {
		t.Errorf("Expected revenue 500, got %d", stats.TotalRevenue)
	}
	if stats.ActiveVouchers != 1 {
		t.Errorf("Expected 1 active voucher, got %d", stats.ActiveVouchers)
	}
	if stats.TotalPrivileges != 1 {
		t.Errorf("Expected 1 privilege, got %d", stats.TotalPrivileges)
	}
	if len(stats.RecentTransactions) == 0 {
		t.Error("Expected recent transactions to be non-empty")
	}
}

func TestGetUsers(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	ensureUser("user-alpha")
	ensureUser("user-beta")
	ensureUser("user-gamma")

	// Give user-alpha a balance
	makeRequest(t, mux, "POST", "/topUp", map[string]interface{}{"uid": "user-alpha", "amount": 3000}, true)

	// Get all users
	resp := makeRequest(t, mux, "POST", "/getUsers", map[string]interface{}{}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var result GetUsersResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if result.Total != 3 {
		t.Errorf("Expected total 3, got %d", result.Total)
	}
	if len(result.Users) != 3 {
		t.Errorf("Expected 3 users, got %d", len(result.Users))
	}

	// Verify user-alpha has correct balance
	for _, u := range result.Users {
		if u.UID == "user-alpha" && u.Balance != 3000 {
			t.Errorf("Expected user-alpha balance 3000, got %d", u.Balance)
		}
	}

	// Test search
	searchResp := makeRequest(t, mux, "POST", "/getUsers", map[string]interface{}{"search": "alpha"}, true)
	var searchResult GetUsersResponse
	json.Unmarshal(searchResp.Body.Bytes(), &searchResult)

	if searchResult.Total != 1 {
		t.Errorf("Expected 1 result for search 'alpha', got %d", searchResult.Total)
	}

	// Test pagination
	pageResp := makeRequest(t, mux, "POST", "/getUsers", map[string]interface{}{"limit": 1, "offset": 0}, true)
	var pageResult GetUsersResponse
	json.Unmarshal(pageResp.Body.Bytes(), &pageResult)

	if len(pageResult.Users) != 1 {
		t.Errorf("Expected 1 user with limit=1, got %d", len(pageResult.Users))
	}
	if pageResult.Total != 3 {
		t.Errorf("Expected total still 3 with pagination, got %d", pageResult.Total)
	}
}

func TestGetUsersEmpty(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	resp := makeRequest(t, mux, "POST", "/getUsers", map[string]interface{}{}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.Code)
		return
	}

	var result GetUsersResponse
	json.Unmarshal(resp.Body.Bytes(), &result)

	if result.Total != 0 {
		t.Errorf("Expected total 0, got %d", result.Total)
	}
}

func TestGetAPIKeys(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	resp := makeRequest(t, mux, "POST", "/getAPIKeys", map[string]interface{}{}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.Code)
		return
	}

	var keys []map[string]interface{}
	json.Unmarshal(resp.Body.Bytes(), &keys)

	if len(keys) < 1 {
		t.Error("Expected at least 1 API key")
		return
	}

	// Key should be masked
	key := keys[0]["key"].(string)
	if key == testAPIKey {
		t.Error("API key should be masked, got full key")
	}
	if len(key) > 11 { // "xxxx...xxxx" = 11 chars max
		t.Errorf("Expected masked key to be short, got %s", key)
	}
}

func TestCreateAPIKey(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	resp := makeRequest(t, mux, "POST", "/createAPIKey", map[string]interface{}{
		"allowed_endpoints": "/getBalance,/topUp",
	}, true)

	if resp.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	var result map[string]interface{}
	json.Unmarshal(resp.Body.Bytes(), &result)

	newKey := result["key"].(string)
	if len(newKey) < 32 {
		t.Errorf("Expected a full API key, got %s", newKey)
	}

	// Verify in database
	var dbKey APIKey
	err := db.Where("key = ?", newKey).First(&dbKey).Error
	if err != nil {
		t.Errorf("API key not found in database: %v", err)
	}
	if dbKey.AllowedEndpoints != "/getBalance,/topUp" {
		t.Errorf("Expected endpoints '/getBalance,/topUp', got %s", dbKey.AllowedEndpoints)
	}
}

func TestCreateAPIKeyMissingEndpoints(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	resp := makeRequest(t, mux, "POST", "/createAPIKey", map[string]interface{}{
		"allowed_endpoints": "",
	}, true)

	if resp.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.Code)
	}
}

func TestDeleteAPIKey(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Create a key to delete
	createResp := makeRequest(t, mux, "POST", "/createAPIKey", map[string]interface{}{
		"allowed_endpoints": "/getBalance",
	}, true)
	var createResult map[string]interface{}
	json.Unmarshal(createResp.Body.Bytes(), &createResult)
	newKey := createResult["key"].(string)

	// Delete it
	resp := makeRequest(t, mux, "POST", "/deleteAPIKey", map[string]interface{}{"key": newKey}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify it's gone
	var dbKey APIKey
	err := db.Where("key = ?", newKey).First(&dbKey).Error
	if err == nil {
		t.Error("API key should have been deleted")
	}

	// Test deleting non-existent key
	resp = makeRequest(t, mux, "POST", "/deleteAPIKey", map[string]interface{}{"key": "nonexistent"}, true)
	if resp.Code != http.StatusNotFound {
		t.Errorf("Expected status 404 for non-existent key, got %d", resp.Code)
	}
}

func TestDeleteAPIKeySelfProtection(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Try to delete the key we're currently using
	resp := makeRequest(t, mux, "POST", "/deleteAPIKey", map[string]interface{}{"key": testAPIKey}, true)

	if resp.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for self-deletion, got %d. Body: %s", resp.Code, resp.Body.String())
	}
}

func TestGetProductMap(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	// Insert products directly
	db.Create(&ProductMap{ID: 1, ProductName: "Coffee"})
	db.Create(&ProductMap{ID: 2, ProductName: "Tea"})

	resp := makeRequest(t, mux, "POST", "/getProductMap", map[string]interface{}{}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.Code)
		return
	}

	var products []ProductMap
	json.Unmarshal(resp.Body.Bytes(), &products)

	if len(products) != 2 {
		t.Errorf("Expected 2 products, got %d", len(products))
	}
}

func TestCreateProductMapping(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	resp := makeRequest(t, mux, "POST", "/createProductMapping", map[string]interface{}{
		"id": 1, "product_name": "Coffee",
	}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
		return
	}

	// Verify in database
	var product ProductMap
	db.First(&product, 1)
	if product.ProductName != "Coffee" {
		t.Errorf("Expected 'Coffee', got %s", product.ProductName)
	}

	// Test upsert
	makeRequest(t, mux, "POST", "/createProductMapping", map[string]interface{}{
		"id": 1, "product_name": "Espresso",
	}, true)

	db.First(&product, 1)
	if product.ProductName != "Espresso" {
		t.Errorf("Expected 'Espresso' after upsert, got %s", product.ProductName)
	}
}

func TestDeleteProductMapping(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	db.Create(&ProductMap{ID: 5, ProductName: "Juice"})

	resp := makeRequest(t, mux, "POST", "/deleteProductMapping", map[string]interface{}{"id": 5}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.Code)
	}

	// Verify deleted
	var product ProductMap
	err := db.First(&product, 5).Error
	if err == nil {
		t.Error("Product should have been deleted")
	}

	// Test non-existent
	resp = makeRequest(t, mux, "POST", "/deleteProductMapping", map[string]interface{}{"id": 999}, true)
	if resp.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", resp.Code)
	}
}

func TestDeleteVoucher(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	ensureUser("voucher-del-user")

	// Create an unused voucher
	voucher := VendVoucher{UID: "voucher-del-user", MachineID: "VM1", Used: false}
	db.Create(&voucher)

	resp := makeRequest(t, mux, "POST", "/deleteVoucher", map[string]interface{}{"id": voucher.ID}, true)
	if resp.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d. Body: %s", resp.Code, resp.Body.String())
	}

	// Verify deleted
	err := db.First(&VendVoucher{}, voucher.ID).Error
	if err == nil {
		t.Error("Voucher should have been deleted")
	}

	// Create a used voucher and try to delete
	usedVoucher := VendVoucher{UID: "voucher-del-user", MachineID: "VM2", Used: true}
	db.Create(&usedVoucher)

	resp = makeRequest(t, mux, "POST", "/deleteVoucher", map[string]interface{}{"id": usedVoucher.ID}, true)
	if resp.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for used voucher, got %d", resp.Code)
	}

	// Test non-existent
	resp = makeRequest(t, mux, "POST", "/deleteVoucher", map[string]interface{}{"id": 99999}, true)
	if resp.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", resp.Code)
	}
}

func TestDeletePrivilege(t *testing.T) {
	mux := setupTestEnvironment(t)
	defer teardownTestEnvironment(t)

	ensureUser("priv-del-user")

	privilege := UserMachinePrivilege{UID: "priv-del-user", MachineID: "VM1", FreeVend: true}
	db.Create(&privilege)

	resp := makeRequest(t, mux, "POST", "/deletePrivilege", map[string]interface{}{
		"uid": "priv-del-user", "machine_id": "VM1",
	}, true)

	if resp.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d. Body: %s", resp.Code, resp.Body.String())
	}

	// Verify deleted
	var p UserMachinePrivilege
	err := db.Where("uid = ? AND machine_id = ?", "priv-del-user", "VM1").First(&p).Error
	if err == nil {
		t.Error("Privilege should have been deleted")
	}

	// Test non-existent
	resp = makeRequest(t, mux, "POST", "/deletePrivilege", map[string]interface{}{
		"uid": "nobody", "machine_id": "nowhere",
	}, true)
	if resp.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", resp.Code)
	}
}
