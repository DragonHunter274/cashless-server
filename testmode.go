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
	"sync"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/golang-jwt/jwt/v5"
)

// TestModeResources holds resources that need cleanup when test mode exits
type TestModeResources struct {
	embeddedPG  *embeddedpostgres.EmbeddedPostgres
	oidcServer  *http.Server
	oidcPort    string
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
	issuer      string
	signingKey  *rsa.PrivateKey
	authCodes   map[string]*authCodeData
	mu          sync.Mutex
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
			Hint: Use "admin" for admin role, or any other username for user role.
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
	if authData.username == "admin" {
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
func setupTestMode() (*TestModeResources, string, error) {
	log.Println("Starting in TEST MODE with embedded PostgreSQL and fake OIDC...")

	resources := &TestModeResources{}

	// Setup embedded PostgreSQL
	embeddedPG, apiKey, err := setupEmbeddedPostgres()
	if err != nil {
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

// setupEmbeddedPostgres starts embedded PostgreSQL and creates test API key
func setupEmbeddedPostgres() (*embeddedpostgres.EmbeddedPostgres, string, error) {
	log.Println("Starting embedded PostgreSQL on port 5434...")

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
