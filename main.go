package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	_ "github.com/dragonhunter274/cashless-server/docs"
)

// @title Cashless Server API
// @version 1.0
// @description Cashless payment server for managing digital wallets, cash purchases, vouchers, and vending machine privileges.
//
// @host localhost:8080
// @BasePath /
//
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key
func main() {
	// Parse command-line flags
	testMode := flag.Bool("test", false, "Run in test mode with embedded PostgreSQL and fake OIDC")
	restoreS3Bucket := flag.String("restore-s3-bucket", "", "S3 bucket name for Barman backup restore (optional)")
	restoreS3Endpoint := flag.String("restore-s3-endpoint", "", "S3 endpoint URL for Barman backup restore (optional)")
	restoreServerName := flag.String("restore-server-name", "", "Server name for Barman backup restore (optional)")
	restoreBackupID := flag.String("restore-backup-id", "", "Backup ID to restore (optional, defaults to latest)")
	restoreDBName := flag.String("restore-db-name", "postgres", "Database name to connect to after restore (default: postgres)")
	flag.Parse()

	var testResources *TestModeResources
	var apiKey string

	if *testMode {
		var err error
		testResources, apiKey, err = setupTestMode(*restoreS3Bucket, *restoreS3Endpoint, *restoreServerName, *restoreBackupID, *restoreDBName)
		if err != nil {
			log.Fatal(err)
		}

		// Display API key prominently
		log.Println("================================================================================")
		log.Println("TEST MODE ACTIVE")
		log.Println("================================================================================")
		log.Println("Embedded PostgreSQL running on port 5434")
		log.Println("Fake OIDC server running on port " + testResources.oidcPort)
		log.Println("================================================================================")
		log.Println("API Key for Web UI:")
		log.Println(apiKey)
		log.Println("================================================================================")
		log.Println("Copy the API key above and paste it into the web UI at http://localhost:8080")
		log.Println("Or click 'Login with SSO' and use username 'admin' or 'user' for testing OIDC")
		log.Println("================================================================================")
	} else {
		if err := initDB(); err != nil {
			log.Fatal(err)
		}
	}

	// Initialize OIDC (optional - disabled if env vars not set)
	if err := initOIDC(); err != nil {
		log.Fatal(err)
	}

	// Start session cleanup goroutine
	startSessionCleanup()

	// Get underlying SQL DB for connection management
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/makePurchase", authMiddleware(makePurchaseHandler))
	mux.HandleFunc("/confirmPurchase", authMiddleware(confirmPurchaseHandler))
	mux.HandleFunc("/getBalance", authMiddleware(getBalanceHandler))
	mux.HandleFunc("/getTransactions", authMiddleware(getTransactionsHandler))
	mux.HandleFunc("/getVouchers", authMiddleware(getVouchersHandler))
	mux.HandleFunc("/getPrivileges", authMiddleware(getPrivilegesHandler))
	mux.HandleFunc("/createUser", authMiddleware(createUserHandler))
	mux.HandleFunc("/createVoucher", authMiddleware(createVoucherHandler))
	mux.HandleFunc("/createPrivilege", authMiddleware(createPrivilegeHandler))
	mux.HandleFunc("/makeCashPurchase", authMiddleware(cashPurchaseHandler))
	mux.HandleFunc("/topUp", authMiddleware(topUpHandler))
	mux.HandleFunc("/getStats", authMiddleware(getStatsHandler))
	mux.HandleFunc("/getUsers", authMiddleware(getUsersHandler))
	mux.HandleFunc("/getAPIKeys", authMiddleware(getAPIKeysHandler))
	mux.HandleFunc("/createAPIKey", authMiddleware(createAPIKeyHandler))
	mux.HandleFunc("/deleteAPIKey", authMiddleware(deleteAPIKeyHandler))
	mux.HandleFunc("/getProductMap", authMiddleware(getProductMapHandler))
	mux.HandleFunc("/createProductMapping", authMiddleware(createProductMappingHandler))
	mux.HandleFunc("/deleteProductMapping", authMiddleware(deleteProductMappingHandler))
	mux.HandleFunc("/deleteVoucher", authMiddleware(deleteVoucherHandler))
	mux.HandleFunc("/deletePrivilege", authMiddleware(deletePrivilegeHandler))
	mux.HandleFunc("/makeRevalue", authMiddleware(makeRevalueHandler))
	mux.HandleFunc("/editTransaction", authMiddleware(editTransactionHandler))
	mux.HandleFunc("/deleteTransaction", authMiddleware(deleteTransactionHandler))
	// OIDC auth routes (no auth required, registered only if OIDC is enabled)
	if oidcEnabled {
		mux.HandleFunc("/auth/login", authLoginHandler)
		mux.HandleFunc("/auth/callback", authCallbackHandler)
		mux.HandleFunc("/auth/logout", authLogoutHandler)
		mux.HandleFunc("/auth/me", authMeHandler)
	}

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/api/v1/read", remoteReadHandler) // Prometheus Remote Read endpoint (no auth required)

	// Swagger UI
	mux.HandleFunc("/swagger/", httpSwagger.WrapHandler)

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

	// Stop test mode resources if running in test mode
	if testResources != nil {
		testResources.Cleanup()
	}

	log.Println("Server stopped")
}
