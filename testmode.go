package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestModeResources holds resources that need cleanup when test mode exits
type TestModeResources struct {
	embeddedPG *embeddedpostgres.EmbeddedPostgres
	oidcServer *http.Server
	oidcPort   string
}

// Cleanup stops all test mode resources
func (r *TestModeResources) Cleanup() {
	if r.embeddedPG != nil {
		log.Println("Stopping embedded PostgreSQL...")
		if err := r.embeddedPG.Stop(); err != nil {
			log.Printf("Error stopping embedded PostgreSQL: %v", err)
		} else {
			log.Println("Embedded PostgreSQL stopped successfully")
		}
	}

	if r.oidcServer != nil {
		log.Println("Stopping fake OIDC server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.oidcServer.Shutdown(ctx); err != nil {
			log.Printf("Error stopping OIDC server: %v", err)
		} else {
			log.Println("Fake OIDC server stopped successfully")
		}
	}
}

// Simple OIDC mock implementation for test mode
type simpleOIDCMock struct {
	issuer     string
	signingKey *rsa.PrivateKey
	authCodes  map[string]*authCodeData
	mu         sync.Mutex
}

type authCodeData struct {
	username    string
	redirectURI string
	scopes      string
	createdAt   time.Time
}

func newSimpleOIDCMock(issuer string, signingKey *rsa.PrivateKey) *simpleOIDCMock {
	return &simpleOIDCMock{
		issuer:     issuer,
		signingKey: signingKey,
		authCodes:  make(map[string]*authCodeData),
	}
}

func (m *simpleOIDCMock) generateAuthCode() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func (m *simpleOIDCMock) discoveryHandler(w http.ResponseWriter, r *http.Request) {
	discovery := map[string]interface{}{
		"issuer":                                m.issuer,
		"authorization_endpoint":                m.issuer + "/authorize",
		"token_endpoint":                        m.issuer + "/token",
		"jwks_uri":                              m.issuer + "/jwks",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"scopes_supported":                      []string{"openid", "profile", "email"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_post", "client_secret_basic"},
		"claims_supported":                      []string{"sub", "name", "email", "groups"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(discovery)
}

func (m *simpleOIDCMock) jwksHandler(w http.ResponseWriter, r *http.Request) {
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "test-key-id",
				"use": "sig",
				"alg": "RS256",
				"n":   base64.RawURLEncoding.EncodeToString(m.signingKey.N.Bytes()),
				"e":   "AQAB",
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jwks)
}

func (m *simpleOIDCMock) authorizeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		// Handle login form submission
		username := r.FormValue("username")
		redirectURI := r.FormValue("redirect_uri")
		state := r.FormValue("state")
		scopes := r.FormValue("scope")

		if username == "" {
			http.Error(w, "Username required", http.StatusBadRequest)
			return
		}

		// Generate authorization code
		code := m.generateAuthCode()
		m.mu.Lock()
		m.authCodes[code] = &authCodeData{
			username:    username,
			redirectURI: redirectURI,
			scopes:      scopes,
			createdAt:   time.Now(),
		}
		m.mu.Unlock()

		// Redirect back to client with code
		redirectURL := redirectURI + "?code=" + code + "&state=" + state
		http.Redirect(w, r, redirectURL, http.StatusFound)
		return
	}

	// Show login form
	redirectURI := r.URL.Query().Get("redirect_uri")
	state := r.URL.Query().Get("state")
	scope := r.URL.Query().Get("scope")

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
	<title>Test OIDC Login</title>
	<style>
		body {
			font-family: Arial, sans-serif;
			display: flex;
			justify-content: center;
			align-items: center;
			height: 100vh;
			margin: 0;
			background-color: #f5f5f5;
		}
		.login-box {
			background: white;
			padding: 30px;
			border-radius: 8px;
			box-shadow: 0 2px 10px rgba(0,0,0,0.1);
			width: 300px;
		}
		h2 {
			margin-top: 0;
			color: #333;
		}
		input {
			width: 100%%;
			padding: 10px;
			margin: 10px 0;
			border: 1px solid #ddd;
			border-radius: 4px;
			box-sizing: border-box;
		}
		button {
			width: 100%%;
			padding: 10px;
			background-color: #4CAF50;
			color: white;
			border: none;
			border-radius: 4px;
			cursor: pointer;
			font-size: 16px;
		}
		button:hover {
			background-color: #45a049;
		}
		.hint {
			font-size: 12px;
			color: #666;
			margin-top: 10px;
		}
	</style>
</head>
<body>
	<div class="login-box">
		<h2>Test Mode Login</h2>
		<form method="POST" action="/authorize">
			<input type="hidden" name="redirect_uri" value="%s">
			<input type="hidden" name="state" value="%s">
			<input type="hidden" name="scope" value="%s">
			<input type="text" name="username" placeholder="Enter username" required autofocus>
			<button type="submit">Login</button>
		</form>
		<div class="hint">
			Hint: Use "superadmin" for super admin, "admin" for admin, or any other username for user role.
		</div>
	</div>
</body>
</html>`, redirectURI, state, scope)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func (m *simpleOIDCMock) tokenHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.ParseForm()
	code := r.FormValue("code")
	grantType := r.FormValue("grant_type")

	if grantType != "authorization_code" {
		http.Error(w, "Unsupported grant type", http.StatusBadRequest)
		return
	}

	// Look up authorization code
	m.mu.Lock()
	authData, ok := m.authCodes[code]
	if ok {
		delete(m.authCodes, code) // Use code only once
	}
	m.mu.Unlock()

	if !ok || time.Since(authData.createdAt) > 10*time.Minute {
		http.Error(w, "Invalid or expired authorization code", http.StatusBadRequest)
		return
	}

	// Generate tokens
	now := time.Now()
	accessToken := m.generateAuthCode() // Simple opaque token for access

	// Determine role based on username
	var groups []string
	if authData.username == "superadmin" {
		groups = []string{"superadmin"}
	} else if authData.username == "admin" {
		groups = []string{"admin"}
	} else {
		groups = []string{"user"}
	}

	// Create ID token
	idToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":    m.issuer,
		"sub":    authData.username,
		"aud":    "test-client",
		"exp":    now.Add(1 * time.Hour).Unix(),
		"iat":    now.Unix(),
		"name":   authData.username,
		"email":  authData.username + "@test.local",
		"groups": groups,
	})

	idTokenString, err := idToken.SignedString(m.signingKey)
	if err != nil {
		http.Error(w, "Failed to sign token", http.StatusInternalServerError)
		return
	}

	// Return tokens
	response := map[string]interface{}{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     idTokenString,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// setupTestMode initializes test mode with embedded PostgreSQL and fake OIDC
func setupTestMode(s3Bucket, s3Endpoint, serverName, backupID, dbName string) (*TestModeResources, string, error) {
	log.Println("Starting in TEST MODE with embedded PostgreSQL and fake OIDC...")

	resources := &TestModeResources{}

	var dataDir string
	if s3Bucket != "" && serverName != "" {
		var err error
		dataDir, err = restoreFromBackup(s3Bucket, s3Endpoint, serverName, backupID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to restore from backup: %v", err)
		}
		log.Printf("Restored database to %s", dataDir)
	}

	// Setup embedded PostgreSQL
	embeddedPG, apiKey, err := setupEmbeddedPostgres(dataDir, dbName)
	if err != nil {
		if dataDir != "" {
			os.RemoveAll(dataDir)
		}
		return nil, "", err
	}
	resources.embeddedPG = embeddedPG

	// Setup fake OIDC server
	oidcServer, oidcPort, err := setupFakeOIDC()
	if err != nil {
		resources.Cleanup()
		return nil, "", err
	}
	resources.oidcServer = oidcServer
	resources.oidcPort = oidcPort

	return resources, apiKey, nil
}

func restoreFromBackup(s3Bucket, s3Endpoint, serverName, backupID string) (string, error) {
	log.Println("Restoring database from Barman backup...")

	// Create temporary directory for restored data
	dataDir, err := os.MkdirTemp("", "cashless-restore-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %v", err)
	}

	// Construct barman-cloud-restore command
	args := []string{"barman-cloud-restore"}
	if s3Endpoint != "" {
		args = append(args, "--endpoint-url", s3Endpoint)
	}

	// If backupID is not provided, fetch the latest one
	if backupID == "" {
		var err error
		backupID, err = getLatestBackupID(s3Bucket, s3Endpoint, serverName)
		if err != nil {
			return "", fmt.Errorf("failed to determine latest backup ID: %v", err)
		}
		log.Printf("Using latest backup ID: %s", backupID)
	}

	args = append(args, fmt.Sprintf("s3://%s", s3Bucket), serverName, backupID, dataDir)

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("Executing: %v", cmd.Args)
	if err := cmd.Run(); err != nil {
		os.RemoveAll(dataDir)
		return "", fmt.Errorf("barman-cloud-restore failed: %v", err)
	}

	// Check for PG_VERSION or postgresql.conf to detect subdirectory
	if _, err := os.Stat(filepath.Join(dataDir, "postgresql.conf")); os.IsNotExist(err) {
		log.Printf("DEBUG: postgresql.conf not found in root. Searching subdirectories...")
		entries, err := os.ReadDir(dataDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() {
					subDirPath := filepath.Join(dataDir, e.Name())
					if _, err := os.Stat(filepath.Join(subDirPath, "postgresql.conf")); err == nil {
						log.Printf("DEBUG: Found postgresql.conf in %s. Moving contents to root...", subDirPath)
						// Move contents of subDirPath to dataDir
						subEntries, _ := os.ReadDir(subDirPath)
						for _, se := range subEntries {
							oldPath := filepath.Join(subDirPath, se.Name())
							newPath := filepath.Join(dataDir, se.Name())
							if err := os.Rename(oldPath, newPath); err != nil {
								log.Printf("WARNING: Failed to move %s: %v", se.Name(), err)
							}
						}
						os.Remove(subDirPath) // Remove empty subdir
						break
					}
				}
			}
		}
	}

	// Check again
	confPath := filepath.Join(dataDir, "postgresql.conf")
	if _, err := os.Stat(confPath); err == nil {
		log.Printf("DEBUG: Validated postgresql.conf exists in %s", dataDir)

		// Sanitize postgresql.conf to remove absolute paths to missing certs
		// and disable SSL to allow local startup
		if err := sanitizePostgresConfig(confPath, s3Bucket, s3Endpoint, serverName); err != nil {
			log.Printf("WARNING: Failed to sanitize postgresql.conf: %v", err)
		}
	} else {
		log.Printf("WARNING: postgresql.conf still not found in %s", dataDir)
	}

	// Create a dummy identity map file if it doesn't exist to avoid startup warning/error
	if _, err := os.Stat(filepath.Join(dataDir, "pg_ident.conf")); os.IsNotExist(err) {
		os.WriteFile(filepath.Join(dataDir, "pg_ident.conf"), []byte(""), 0600)
	}

	// Ensure recovery signals are handled
	// Write a recovery.signal file to tell Postgres to go into recovery mode
	if err := os.WriteFile(filepath.Join(dataDir, "recovery.signal"), []byte(""), 0600); err != nil {
		log.Printf("WARNING: Failed to create recovery.signal: %v", err)
	}

	// Create a permissive pg_hba.conf to ensure we can connect
	// overwrite the existing one to be sure
	hbaContent := "local all all trust\nhost all all 127.0.0.1/32 trust\nhost all all ::1/128 trust\n"
	if err := os.WriteFile(filepath.Join(dataDir, "pg_hba.conf"), []byte(hbaContent), 0600); err != nil {
		log.Printf("WARNING: Failed to write pg_hba.conf: %v", err)
	}

	return dataDir, nil
}

func sanitizePostgresConfig(confPath, s3Bucket, s3Endpoint, serverName string) error {
	content, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	var newLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Comment out configs that we will replace or that cause issues
		if strings.HasPrefix(trimmed, "ssl") ||
			strings.HasPrefix(trimmed, "archive_command") ||
			strings.HasPrefix(trimmed, "restore_command") ||
			strings.HasPrefix(trimmed, "recovery_target") ||
			strings.HasPrefix(trimmed, "shared_preload_libraries") ||
			strings.HasPrefix(trimmed, "logging_collector") ||
			strings.HasPrefix(trimmed, "log_directory") ||
			strings.HasPrefix(trimmed, "log_filename") ||
			strings.HasPrefix(trimmed, "log_destination") ||
			strings.HasPrefix(trimmed, "unix_socket_directories") {
			newLines = append(newLines, "# "+line)
		} else {
			newLines = append(newLines, line)
		}
	}

	// properties to enforce
	newLines = append(newLines, "ssl = off")
	newLines = append(newLines, "shared_preload_libraries = ''") // Clear libraries that might not exist locally
	newLines = append(newLines, "archive_mode = off")
	newLines = append(newLines, "logging_collector = off") // Disable file logging to avoid path issues
	newLines = append(newLines, "listen_addresses = 'localhost'")
	newLines = append(newLines, "port = 5434")
	newLines = append(newLines, "unix_socket_directories = '/tmp'")

	// Recovery Configuration
	// Construct restore_command using the same flags we used for fetching the backup
	// barman-cloud-wal-restore --endpoint-url [ENDPOINT] s3://[BUCKET] [SERVER] %f %p
	restoreCmd := "barman-cloud-wal-restore"
	if s3Endpoint != "" {
		restoreCmd += fmt.Sprintf(" --endpoint-url %s", s3Endpoint)
	}
	restoreCmd += fmt.Sprintf(" s3://%s %s %%f %%p", s3Bucket, serverName)

	newLines = append(newLines, fmt.Sprintf("restore_command = '%s'", restoreCmd))
	newLines = append(newLines, "recovery_target_action = 'promote'")
	// We might not want recovery_target = 'immediate' if we want it to catch up completely?
	// Usually default is to recover to the end of WAL found.

	return os.WriteFile(confPath, []byte(strings.Join(newLines, "\n")), 0600)
}

func getLatestBackupID(s3Bucket, s3Endpoint, serverName string) (string, error) {
	args := []string{"barman-cloud-backup-list"}
	if s3Endpoint != "" {
		args = append(args, "--endpoint-url", s3Endpoint)
	}
	args = append(args, "--format", "json", fmt.Sprintf("s3://%s", s3Bucket), serverName)

	cmd := exec.Command(args[0], args[1:]...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to list backups: %v\nOutput: %s", err, string(output))
	}
	log.Printf("DEBUG: barman-cloud-backup-list output:\n%s", string(output))

	// Parse JSON output
	// Expected format: {"backups_list": [{"backup_id":"...","status":"DONE",...}, ...]}
	type Backup struct {
		ID     string `json:"backup_id"`
		Status string `json:"status"`
	}

	type BackupListResponse struct {
		BackupsList []Backup `json:"backups_list"`
	}

	var response BackupListResponse
	if err := json.Unmarshal(output, &response); err == nil && len(response.BackupsList) > 0 {
		var validBackups []Backup
		for _, b := range response.BackupsList {
			if b.Status == "DONE" {
				validBackups = append(validBackups, b)
			}
		}

		if len(validBackups) == 0 {
			return "", fmt.Errorf("no valid 'DONE' backups found in JSON list")
		}

		latest := validBackups[0].ID
		for _, b := range validBackups {
			if b.ID > latest {
				latest = b.ID
			}
		}
		log.Printf("Found latest backup ID from JSON list: %s", latest)
		return latest, nil
	}

	// Fallback to text parsing or direct array if the wrapper is missing (backwards compat)
	// But based on user output, it's definitely wrapped.
	// Let's keep the array fallback just in case, but using the wrapper struct reference
	var directBackups []Backup
	if err := json.Unmarshal(output, &directBackups); err == nil && len(directBackups) > 0 {
		response.BackupsList = directBackups
	}

	if len(response.BackupsList) == 0 {
		// Try parsing lines if JSON failed completely (e.g. non-JSON error output caught as output)
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) > 0 && strings.HasPrefix(fields[0], "2") {
				response.BackupsList = append(response.BackupsList, Backup{ID: fields[0], Status: "UNKNOWN"})
			}
		}
	}

	if len(response.BackupsList) == 0 {
		return "", fmt.Errorf("failed to parse backups list (JSON mismatch and text fallback failed)")
	}

	// Re-run filter logic for fallbacks
	var validBackups []Backup
	for _, b := range response.BackupsList {
		if b.Status == "DONE" || b.Status == "UNKNOWN" {
			validBackups = append(validBackups, b)
		}
	}

	if len(validBackups) == 0 {
		return "", fmt.Errorf("no valid 'DONE' backups found")
	}

	latest := validBackups[0].ID
	for _, b := range validBackups {
		if b.ID > latest {
			latest = b.ID
		}
	}

	return latest, nil
}

// setupEmbeddedPostgres starts embedded PostgreSQL and creates test API key
func setupEmbeddedPostgres(dataDir, dbName string) (*embeddedpostgres.EmbeddedPostgres, string, error) {
	log.Println("Starting embedded PostgreSQL on port 5434...")

	// Default to "postgres" or "cashless_test" if not provided, but here we expect caller to provide it or we default before call.
	// main.go defaults it to "postgres".
	if dbName == "" {
		dbName = "cashless_test" // Fallback similar to original code if not restoring?
		// Actually original code used "cashless_test" before my edits.
		// If restoring, user probably wants "app".
		// If NOT restoring, we want "cashless_test".
		// Let's rely on main.go passing something.
	}

	// If NOT restoring (dataDir empty), we might want to stick to "cashless_test" to avoid conflict with "postgres" system db?
	// But let's respect the flag.

	config := embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V16).
		Port(5434).
		Database(dbName).
		Username("postgres").
		Password("postgres")

	if dataDir != "" {
		config = config.DataPath(dataDir)
	}

	// Start embedded PostgreSQL on port 5434 to avoid conflicts
	embeddedPG := embeddedpostgres.NewDatabase(config)

	if err := embeddedPG.Start(); err != nil {
		return nil, "", fmt.Errorf("failed to start embedded PostgreSQL: %v", err)
	}

	// Set environment variables for database connection
	os.Setenv("PG_USER", "postgres")
	os.Setenv("PG_PASSWORD", "postgres")
	os.Setenv("PG_DBNAME", dbName)
	os.Setenv("PG_HOST", "localhost port=5434")

	// Wait for PostgreSQL to be ready and potentially finish recovery
	log.Println("Waiting for database to become ready and finish recovery...")

	// Open a temporary connection to check recovery status
	dsn := fmt.Sprintf("host=localhost user=postgres password=postgres dbname=%s port=5434 sslmode=disable", dbName)
	tempDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("WARNING: Failed to open temp connection for recovery check: %v", err)
	}

	for i := 0; i < 60; i++ {
		// If tempDB failed (e.g. DB not up yet), try to reconnect
		if tempDB == nil {
			tempDB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
				Logger: logger.Default.LogMode(logger.Silent),
			})
		}

		if tempDB != nil {
			var inRecovery bool
			if err := tempDB.Raw("SELECT pg_is_in_recovery()").Scan(&inRecovery).Error; err != nil {
				log.Printf("Waiting for DB connection... (%v)", err)
			} else {
				if !inRecovery {
					log.Println("Database is in read-write mode (recovery finished).")
					break
				}
				log.Println("Database is still in recovery mode (read-only). Waiting...")
			}
		} else {
			log.Println("Waiting for DB to start...")
		}
		time.Sleep(2 * time.Second)
	}

	// Double check if we are still in recovery
	if tempDB != nil {
		var inRecovery bool
		tempDB.Raw("SELECT pg_is_in_recovery()").Scan(&inRecovery)
		if inRecovery {
			log.Printf("WARNING: Database is still in recovery mode after timeout. initDB might fail.")
		}
	}

	// If we restored from backup, we don't need to initDB or create API key if they exist
	// But initDB creates tables if not exists, so it should be safe?
	// The prompt implies restoring the DB state, so migrations might already be applied.
	// However, we still need the API key for the test runner to work.
	// Let's assume we might need to regenerate key or check if one exists.
	// For now, running initDB is safer to ensure schema is up to date if backup is old.

	// Initialize database
	if err := initDB(); err != nil {
		embeddedPG.Stop()
		return nil, "", fmt.Errorf("failed to initialize database: %v", err)
	}

	// DIAGNOSTIC LOGGING
	var currentDB string
	db.Raw("SELECT current_database()").Scan(&currentDB)
	log.Printf("DIAGNOSTIC: Connected to database: %s", currentDB)

	var tables []string
	db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'").Scan(&tables)
	log.Printf("DIAGNOSTIC: Tables in public schema: %v", tables)

	var userCount int64
	if err := db.Table("users").Count(&userCount).Error; err != nil {
		log.Printf("DIAGNOSTIC: Failed to count users: %v", err)
	} else {
		log.Printf("DIAGNOSTIC: Users count: %d", userCount)
	}

	var txCount int64
	if err := db.Table("transactions").Count(&txCount).Error; err != nil {
		log.Printf("DIAGNOSTIC: Failed to count transactions: %v", err)
	} else {
		log.Printf("DIAGNOSTIC: Transactions count: %d", txCount)

		// If transactions exist, print sample to check UID
		if txCount > 0 {
			var sampleTx []map[string]interface{}
			db.Table("transactions").Limit(5).Find(&sampleTx)
			log.Printf("DIAGNOSTIC: Sample transactions: %+v", sampleTx)
		}
	}

	// Generate and create API key
	apiKey := generateAPIKey()
	key := APIKey{
		Key:              apiKey,
		AllowedEndpoints: "/makePurchase,/confirmPurchase,/makeCashPurchase,/getBalance,/getTransactions,/getVouchers,/getPrivileges,/topUp,/createUser,/createVoucher,/createPrivilege,/getStats,/getUsers,/getAPIKeys,/createAPIKey,/deleteAPIKey,/getProductMap,/createProductMapping,/deleteProductMapping,/deleteVoucher,/deletePrivilege,/editTransaction,/deleteTransaction",
	}
	if err := db.Create(&key).Error; err != nil {
		embeddedPG.Stop()
		return nil, "", fmt.Errorf("failed to create API key: %v", err)
	}

	log.Println("Embedded PostgreSQL started successfully")
	return embeddedPG, apiKey, nil
}

// setupFakeOIDC starts a fake OIDC provider for testing
func setupFakeOIDC() (*http.Server, string, error) {
	oidcPort := "7835"
	oidcIssuer := fmt.Sprintf("http://localhost:%s", oidcPort)

	log.Printf("Starting fake OIDC server on port %s...", oidcPort)

	// Generate RSA key for JWT signing
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate RSA key: %v", err)
	}

	// Create simple OIDC mock
	mock := newSimpleOIDCMock(oidcIssuer, key)

	// Create HTTP server for OIDC provider
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", mock.discoveryHandler)
	mux.HandleFunc("/jwks", mock.jwksHandler)
	mux.HandleFunc("/authorize", mock.authorizeHandler)
	mux.HandleFunc("/token", mock.tokenHandler)

	server := &http.Server{
		Addr:    ":" + oidcPort,
		Handler: mux,
	}

	// Start server in background
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("OIDC server error: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(500 * time.Millisecond)

	// Set environment variables for OIDC
	os.Setenv("OIDC_ISSUER", oidcIssuer)
	os.Setenv("OIDC_CLIENT_ID", "test-client")
	os.Setenv("OIDC_CLIENT_SECRET", "test-secret")
	os.Setenv("OIDC_REDIRECT_URL", "http://localhost:8080/auth/callback")
	os.Setenv("OIDC_ADMIN_CLAIM", "groups")
	os.Setenv("OIDC_ADMIN_VALUE", "admin")
	os.Setenv("OIDC_SCOPES", "openid,profile,email")
	os.Setenv("OIDC_SESSION_TTL", "24h")

	log.Printf("Fake OIDC server started successfully at %s", oidcIssuer)
	log.Printf("OIDC discovery endpoint: %s/.well-known/openid-configuration", oidcIssuer)
	return server, oidcPort, nil
}
