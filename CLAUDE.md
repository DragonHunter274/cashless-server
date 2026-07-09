# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a cashless payment server written in Go that manages digital wallet transactions, cash purchases, vouchers, and user privileges for vending machines. It uses PostgreSQL for data persistence and exposes Prometheus metrics for monitoring.

## Development Commands

### Building and Running

```bash
# Build the application
go build -o cashless-server main.go

# Run in TEST MODE (easiest for development/testing)
# This starts an embedded PostgreSQL database and auto-generates an API key
./cashless-server -test

# Run in PRODUCTION MODE (requires PostgreSQL environment variables)
go run main.go

# Build Docker image
docker build -t cashless-server .

# Run in Docker
docker run -p 8080:8080 \
  -e PG_USER=<user> \
  -e PG_PASSWORD=<password> \
  -e PG_DBNAME=<database> \
  -e PG_HOST=<host> \
  cashless-server
```

### Test Mode

**Test mode** is the easiest way to run the application for development and testing:

```bash
./cashless-server -test
```

When running in test mode:
- An embedded PostgreSQL database is automatically started on port 5434
- A random API key is generated and displayed in the console
- No external PostgreSQL installation required
- Perfect for trying out the web UI or running quick tests
- Database is stored in memory and cleared when the server stops
- **Graceful shutdown**: Press Ctrl+C to stop the server - it will automatically clean up and stop PostgreSQL

**Example output:**
```
================================================================================
TEST MODE ACTIVE - Embedded PostgreSQL running on port 5434
================================================================================
API Key for Web UI:
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6a7b8c9d0e1f2
================================================================================
Copy the API key above and paste it into the web UI at http://localhost:8080
================================================================================
```

### Required Environment Variables (Production Mode)

When running in production mode, the application requires these PostgreSQL connection environment variables:
- `PG_USER` - PostgreSQL username
- `PG_PASSWORD` - PostgreSQL password
- `PG_DBNAME` - Database name
- `PG_HOST` - PostgreSQL host (e.g., `localhost:5432`)

### OIDC Authentication (Optional)

The server supports OpenID Connect (OIDC) authentication for admin and user login via SSO. OIDC is **completely optional** - if not configured, the server works exactly as before with API key authentication only.

**OIDC Environment Variables:**

All OIDC variables are optional. If `OIDC_ISSUER` is not set, OIDC authentication is disabled:

- `OIDC_ISSUER` - OIDC provider URL (e.g., `https://auth.example.com/realms/myrealm`). If empty, OIDC is disabled.
- `OIDC_CLIENT_ID` - OAuth2 client ID
- `OIDC_CLIENT_SECRET` - OAuth2 client secret
- `OIDC_REDIRECT_URL` - Callback URL (default: `http://localhost:8080/auth/callback`)
- `OIDC_ADMIN_CLAIM` - ID token claim to check for admin role (default: `groups`)
- `OIDC_ADMIN_VALUE` - Value in the claim that grants admin role (default: `admin`)
- `OIDC_SCOPES` - Comma-separated OIDC scopes (default: `openid,profile,email`)
- `OIDC_SESSION_TTL` - Session duration (default: `24h`)

**How OIDC Works:**

1. Users click "Login with SSO" in the web UI
2. They're redirected to the OIDC provider to authenticate
3. After successful authentication, a session is created and stored in the database
4. The session cookie is HttpOnly and includes the user's role (admin or user)
5. Sessions expire after the configured TTL (default 24 hours)
6. Expired sessions are automatically cleaned up every hour

**Dual Authentication:**

The server supports **both** authentication methods simultaneously:
- **API key authentication** - Traditional header-based auth (always available)
- **OIDC session authentication** - Cookie-based SSO auth (only when OIDC is configured)

API key authentication takes priority - if an `X-API-Key` header is present, it's used regardless of session cookies.

**Example OIDC Setup (Keycloak):**

```bash
export OIDC_ISSUER=https://keycloak.example.com/realms/cashless
export OIDC_CLIENT_ID=cashless-server
export OIDC_CLIENT_SECRET=your-client-secret
export OIDC_REDIRECT_URL=https://cashless.example.com/auth/callback
export OIDC_ADMIN_CLAIM=groups
export OIDC_ADMIN_VALUE=cashless-admin

./cashless-server
```

**Role-Based Access:**

- **Admin role**: Granted when the OIDC ID token contains the admin value in the configured claim (e.g., `"admin"` in the `groups` claim). Admins have full access to all endpoints.
- **User role**: All other authenticated users. Currently has the same access as admins, but the infrastructure is in place for future role-based restrictions.

### Firmware Signature Verification (Optional)

By default the server accepts any uploaded firmware.img that's at least 512 bytes (the embedded signature size) without checking the signature itself - devices verify it before flashing regardless. To also fail fast on bad uploads at the server, set:

- `FIRMWARE_RSA_PUBLIC_KEY_PATH` - Path to a PEM-encoded RSA public key (the same `rsa_key.pub` embedded in device firmware, see [spec/ota.md](spec/ota.md)). When set, `/uploadFirmware` verifies the embedded RSA-SHA256 signature and rejects the upload on mismatch.

The private key never needs to be provided to or stored on the server.

## Architecture

### File Organization

All code lives in the `main` package, split by responsibility:

- [main.go](main.go) - flag parsing, route registration, server startup/shutdown
- [models.go](models.go) - GORM models, request/response structs, package-level state (`db`, OIDC config vars)
- [db.go](db.go) - `initDB`, `ensureUser`, `generateAPIKey`
- [auth.go](auth.go) - OIDC setup, `authMiddleware`, session cleanup, `/auth/*` handlers
- [handlers_purchase.go](handlers_purchase.go) - purchase/top-up/revalue/transaction handlers
- [handlers_user.go](handlers_user.go) - balance, user creation, stats, user listing handlers
- [handlers_voucher.go](handlers_voucher.go) - voucher and privilege handlers
- [handlers_admin.go](handlers_admin.go) - API key and product map handlers
- [handlers_firmware.go](handlers_firmware.go) - firmware upload/activation/deletion and the public OTA manifest/binary endpoints
- [middleware.go](middleware.go) - `corsMiddleware`, `ternary` helper
- [metrics.go](metrics.go) - Prometheus `PurchaseCollector`
- [remote_read.go](remote_read.go) - Prometheus remote-read endpoint
- [testmode.go](testmode.go) - embedded Postgres + fake OIDC for `-test` mode

### Database Schema

The application manages eight PostgreSQL tables (auto-created on startup):

1. **users** - User accounts with UIDs
2. **transactions** - All financial transactions (top-ups, purchases, refunds)
3. **user_machine_privileges** - Per-machine free vend privileges for specific users
4. **vend_vouchers** - Single-use vouchers for free vends on specific machines
5. **api_keys** - API authentication with endpoint-level permissions
6. **product_map** - Maps product IDs to human-readable product names
7. **sessions** - OIDC session tokens with user info and expiration (only used when OIDC is enabled)
8. **firmware** - Uploaded signed firmware images (version, binary blob, active flag) for OTA updates

### Transaction States and Workflow

Transactions have three states:
- `pending` - Created but not confirmed (60-second timeout)
- `confirmed` - Successfully completed
- `failed` - Timed out or explicitly failed

**Important**: When a purchase is created via `/makePurchase`, it starts as `pending` and automatically transitions to `failed` after 60 seconds unless confirmed via `/confirmPurchase`. This two-phase commit prevents dispensing failures from debiting accounts.

### Payment Methods and Types

Transactions are distinguished by:
- **Payment method** (`payment_method` field): `digital` for UID-based, `cash` for anonymous
- **Cash flag** (`is_cash` field): `true` for cash purchases, `false` for digital transactions
- **Amount sign**: Negative amounts are purchases/debits, positive amounts are top-ups/credits

### Free Vend Logic

The purchase handler ([handlers_purchase.go:212-299](handlers_purchase.go#L212-L299)) checks for free vends in this priority order:
1. **Machine privileges** - If user has `free_vend = true` for that machine
2. **Vouchers** - If user has an unused voucher for that machine
3. **Normal payment** - User balance is checked and debited

When a voucher is used, it's marked as `used = TRUE` immediately during purchase creation, not during confirmation.

### Prometheus Metrics

The application implements a custom Prometheus collector (`PurchaseCollector`) that:
- Exports `purchases_total` counter with labels: `product`, `machine_id`, `method`
- Caches creation timestamps to ensure metric stability (prevents Prometheus errors from changing timestamps)
- Aggregates confirmed purchases from the database
- Uses `MIN(created_at)` to find the first transaction time for each product/machine/method combination

Metrics are exposed at `/metrics` (no API key required).

### API Authentication

All endpoints except `/metrics` require API key authentication via the `X-API-Key` header. API keys are stored in the `api_keys` table with endpoint-level permissions (comma-separated allowed paths).

### CORS Middleware

The server implements permissive CORS ([middleware.go:12-29](middleware.go#L12-L29)):
- Allows all origins (`Access-Control-Allow-Origin: *`)
- Allows `POST`, `GET`, `OPTIONS` methods
- Allows `Content-Type` and `X-API-Key` headers

## Key Implementation Details

### Product Name Resolution

The `get_product_name()` function ([handlers_purchase.go:192-198](handlers_purchase.go#L192-L198)) looks up product names from the `product_map` table. If no mapping exists, it returns the product ID as a string.

### User Creation Pattern

The `ensureUser()` helper ([db.go:66-70](db.go#L66-L70)) uses `FirstOrCreate` to idempotently create users. This is called before any operation that requires a user to exist.

### Pending Transaction Timeout

A goroutine is spawned for each purchase ([handlers_purchase.go:290-295](handlers_purchase.go#L290-L295)) that waits 60 seconds and marks the transaction as `failed` if still `pending`. This prevents zombie pending transactions.

### Database Indexes

The schema includes three performance indexes:
- `idx_transactions_status` - For status-based queries
- `idx_transactions_uid` - For user balance queries
- `idx_transactions_metrics` - Composite index for Prometheus metric aggregation

### Firmware / OTA Updates

Implements the signed OTA firmware distribution described in [spec/ota.md](spec/ota.md) (esp32FOTA-compatible). Signing happens entirely off-server with a private key that never touches the server - admins upload an already-signed `firmware.img` (`[512-byte RSA signature][firmware binary]`) via the web UI, which is stored as a `bytea` blob in the `firmware` table ([handlers_firmware.go](handlers_firmware.go)).

- **Upload vs. activate are separate steps**: `/uploadFirmware` stores a new version but does not publish it; `/activateFirmware` flips exactly one row's `active` flag (deactivating any previously active version) to publish it to devices. This makes rollback a matter of re-activating an older uploaded version.
- **Optional upload-time signature verification**: if `FIRMWARE_RSA_PUBLIC_KEY_PATH` env var points to a PEM-encoded RSA public key (the same `rsa_key.pub` embedded in device firmware), uploads are verified server-side and rejected on mismatch. If unset, verification is skipped - devices still verify the signature themselves before flashing, so this is purely a fail-fast convenience for admins.
- **Device-facing endpoints** (`GET /firmware/manifest.json`, `GET /firmware/firmware.img`) serve the currently active firmware in the exact layout `spec/ota.md` expects: a manifest with `{"type":"esp32-fota-http","version":...,"bin":"firmware.img"}` and the binary at a path relative to the manifest. `/firmware/manifest.json` is public (no auth) so `OTA_MANIFEST_URL` on the device needs no credentials; `/firmware/firmware.img` requires an API key or OIDC session, so devices must be configured with an `X-API-Key` header to download the binary.
- Firmware list/detail responses never include the binary blob (`Data` is `json:"-"` and list queries `.Omit("data")`) to keep admin list calls cheap.

## Endpoints

All endpoints require authentication via `X-API-Key` header or OIDC session cookie, except the public endpoints listed below:

### Core Operations
- `POST /makePurchase` - Create pending purchase (with optional UID)
- `POST /confirmPurchase` - Confirm pending transaction
- `POST /makeCashPurchase` - Create confirmed cash purchase (no UID)
- `POST /getBalance` - Get user balance
- `POST /getTransactions` - Get transaction history (optional UID filter, pagination support)
- `POST /getVouchers` - Get vouchers (optional UID filter)
- `POST /getPrivileges` - Get machine privileges (optional UID filter)
- `POST /topUp` - Add funds to user account
- `POST /createUser` - Create new user
- `POST /createVoucher` - Create voucher for user+machine
- `POST /createPrivilege` - Set free vend privilege for user+machine

### Admin Operations
- `POST /getStats` - Dashboard statistics (user counts, revenue, transaction counts)
- `POST /getUsers` - Paginated user list with computed balances (supports search, limit, offset)
- `POST /getAPIKeys` - List all API keys (keys are masked)
- `POST /createAPIKey` - Create new API key with endpoint permissions
- `POST /deleteAPIKey` - Revoke an API key (prevents self-deletion)
- `POST /getProductMap` - List all product ID to name mappings
- `POST /createProductMapping` - Create or update a product mapping (upsert)
- `POST /deleteProductMapping` - Delete a product mapping
- `POST /deleteVoucher` - Delete an unused voucher (rejects used vouchers)
- `POST /deletePrivilege` - Delete a machine privilege
- `POST /uploadFirmware` - Upload a signed firmware.img for a version (multipart form: `version`, `firmware`); does not activate it
- `POST /getFirmwareList` - List uploaded firmware versions (metadata only, no binary)
- `POST /activateFirmware` - Publish a version as the active firmware served to devices
- `POST /deleteFirmware` - Delete an uploaded, non-active firmware version
- `GET /firmware/firmware.img` - Download the active signed firmware binary (requires `X-API-Key`, unlike the manifest)

### OIDC Authentication Endpoints (no auth, only registered when OIDC is enabled)
- `GET /auth/login` - Initiate OIDC login flow (redirects to OIDC provider)
- `GET /auth/callback` - OIDC callback handler (processes auth code, creates session, redirects to /)
- `POST /auth/logout` - Logout (deletes session, clears cookie)
- `GET /auth/me` - Get current session info (returns `{authenticated, email, name, role}`)

### Public Endpoints (no auth)
- `GET /metrics` - Prometheus metrics
- `GET /` - Web admin frontend
- `GET /firmware/manifest.json` - OTA manifest for the active firmware (consumed directly by devices, see spec/ota.md)

## Web Frontend

The server includes a modern single-page web application for managing all aspects of the cashless system ([static/index.html](static/index.html)).

### Features

- **User Management**: Create users, check balances, top up accounts
- **User Info Page**: Comprehensive view of a user's balance, vouchers, privileges, and recent transactions
- **Purchase Management**: Create digital purchases (with pending confirmation), confirm purchases, record cash purchases
- **Voucher Management**: Create single-use vouchers for specific users and machines
- **Privilege Management**: Grant free vend privileges to users on specific machines
- **Transaction History**: View all transactions with filtering by user and pagination
- **Firmware Management**: Upload signed firmware images and activate/roll back which version is served to devices
- **Responsive Design**: Works on desktop, tablet, and mobile devices

### Access

The web frontend is served at the root URL (`http://localhost:8080`) and supports two authentication methods:
- **API Key**: Enter an API key in the topbar input field
- **OIDC SSO**: Click "Login with SSO" button (only shown when OIDC is configured)

Both methods provide full access to all features. Sessions and API keys are managed transparently - you can use either method or both simultaneously.

### Static Files

Static files are served from the `./static` directory ([main.go:123-124](main.go#L123-L124)). The server uses Go's built-in `http.FileServer` to serve the web frontend.

## Deployment

The application is containerized and automatically built/pushed to GitHub Container Registry via GitHub Actions ([.github/workflows/build-and-push.yaml](.github/workflows/build-and-push.yaml)) on pushes to `main`. The workflow builds multi-platform images (amd64 and arm64).

## Testing

### Test Suite

The project includes a comprehensive test suite ([main_test.go](main_test.go)) that uses an embedded PostgreSQL database for integration testing. All 16 tests must pass before merging changes.

**Running tests:**
```bash
go test -v -timeout 5m
```

### Test Infrastructure

- **Embedded PostgreSQL**: Tests use `github.com/fergusstrange/embedded-postgres` to create an isolated test database on port 5433
- **Database cleanup**: Each test gets a clean database state by deleting all data before running
- **API key setup**: A test API key with full permissions is automatically created for each test
- **Test helpers**: `makeRequest()` helper simplifies HTTP testing with automatic API key injection

### Adding Tests for New Endpoints

**IMPORTANT**: Whenever you create a new API endpoint, you MUST add corresponding tests to [main_test.go](main_test.go). Follow these guidelines:

1. **Understand the endpoint's actual behavior** - Read the handler code to determine:
   - HTTP status code returned (200, 201, 403, etc.)
   - Response format (JSON, plain text, or no body)
   - Side effects (database changes, background goroutines)

2. **Test the actual API contract** - Your test should match what the handler actually does:
   - If the handler returns `http.StatusCreated` (201), test for 201, not 200
   - If the handler returns plain text, don't try to parse it as JSON
   - If the handler returns no body, verify database changes instead
   - If the handler returns JSON, check only the fields that are actually returned

3. **Use database verification** - Many handlers return minimal responses (just status codes). Verify the operation succeeded by querying the database directly:
   ```go
   var user User
   err := db.Where("uid = ?", "test-uid").First(&user).Error
   if err != nil {
       t.Errorf("User was not created: %v", err)
   }
   ```

4. **Test error cases** - Include tests for:
   - Missing required fields (should return 400 Bad Request)
   - Insufficient funds/permissions (check actual error status code)
   - Invalid data formats

5. **Follow naming convention** - Name tests as `TestEndpointName`, e.g., `TestMakePurchase`, `TestCreateVoucher`

6. **Include in test flow** - Add your test to ensure it runs with `go test -v`

### Example Test Pattern

Here's the typical pattern for testing an endpoint that returns no body:

```go
func TestYourEndpoint(t *testing.T) {
    mux := setupTestEnvironment(t)
    defer teardownTestEnvironment(t)

    reqBody := map[string]interface{}{
        "field": "value",
    }

    resp := makeRequest(t, mux, "POST", "/yourEndpoint", reqBody, true)

    // Check actual status code returned by handler
    if resp.Code != http.StatusOK {
        t.Errorf("Expected status 200, got %d. Body: %s", resp.Code, resp.Body.String())
        return
    }

    // Verify database changes
    var record YourModel
    err := db.Where("field = ?", "value").First(&record).Error
    if err != nil {
        t.Errorf("Record was not created: %v", err)
    }
}
```

### Common Test Patterns

- **201 Created + JSON response**: Cash purchases, some creation endpoints
- **200 OK + no body**: User creation, voucher creation, privilege creation, purchase confirmation
- **200 OK + JSON response**: Balance queries, successful digital purchases
- **403 Forbidden + error text**: Insufficient balance, permission denied
- **400 Bad Request + error text**: Invalid input, missing required fields

## Known Issues and Fixes

- **Voucher marking fix** ([handlers_purchase.go:282-286](handlers_purchase.go#L282-L286)): The code uses a subquery to select a specific voucher ID before marking it used, avoiding potential issues with PostgreSQL's lack of `LIMIT` support in `UPDATE` statements.
