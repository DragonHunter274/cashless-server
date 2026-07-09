package main

import (
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
	"gorm.io/gorm"
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
	ID            uint      `gorm:"primaryKey" json:"transaction_id"`
	UID           *string   `json:"uid"`
	Amount        int       `json:"amount"`
	Product       string    `json:"product"`
	Status        string    `json:"status"`
	PaymentMethod string    `gorm:"column:payment_method" json:"payment_method"`
	MachineID     string    `gorm:"column:machine_id" json:"machine_id"`
	IsCash        bool      `gorm:"default:false" json:"is_cash"`
	CreatedAt     time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
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

func (ProductMap) TableName() string {
	return "product_map"
}

type Firmware struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Version   string    `gorm:"uniqueIndex;size:64" json:"version"`
	Filename  string    `json:"filename"`
	Data      []byte    `json:"-"`
	Size      int       `json:"size"`
	Active    bool      `gorm:"default:false" json:"active"`
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (Firmware) TableName() string {
	return "firmware"
}

type Session struct {
	Token     string `gorm:"primaryKey;size:64"`
	Email     string
	Name      string
	Subject   string // OIDC sub claim
	Role      string // "admin" or "user"
	ExpiresAt time.Time
	CreatedAt time.Time `gorm:"default:CURRENT_TIMESTAMP"`
}

// Auth context types
type contextKey string

const authContextKey contextKey = "auth"

type authInfo struct {
	Method string // "apikey" or "oidc"
	Role   string // "admin" or "user"
	Email  string
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

type FirmwareVersionRequest struct {
	Version string `json:"version"`
}

type RevalueRequest struct {
	UID       string `json:"uid"`
	Amount    int    `json:"amount"`
	MachineID string `json:"machine_id"`
	SessionID string `json:"session_id"`
}

type EditTransactionRequest struct {
	ID            uint    `json:"id"`
	UID           *string `json:"uid"`
	Amount        int     `json:"amount"`
	Product       string  `json:"product"`
	Status        string  `json:"status"`
	PaymentMethod string  `json:"payment_method"`
	MachineID     string  `json:"machine_id"`
}

var db *gorm.DB

// OIDC state
var (
	oidcEnabled         bool
	oidcProvider        *oidc.Provider
	oauth2Config        *oauth2.Config
	oidcVerifier        *oidc.IDTokenVerifier
	oidcAdminClaim      string
	oidcAdminValue      string
	oidcSuperadminValue string
	sessionTTL          time.Duration
)
