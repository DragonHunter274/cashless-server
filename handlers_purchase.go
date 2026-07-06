package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"gorm.io/gorm"
)

// @Summary Top up a user's balance
// @Tags Users
// @Accept json
// @Param body body TopUpRequest true "Top-up details"
// @Success 201 {string} string "Top-up successful"
// @Failure 400 {string} string "Invalid top-up data"
// @Security ApiKeyAuth
// @Router /topUp [post]
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

// @Summary Revalue (top up) via vending machine
// @Description Accepts a revalue request from a vending machine. Each coin/bill insertion triggers a separate call. The session_id groups revalue entries belonging to the same session.
// @Tags Users
// @Accept json
// @Produce json
// @Param body body RevalueRequest true "Revalue details"
// @Success 200 {object} map[string]interface{} "success, new_balance"
// @Failure 400 {object} map[string]interface{} "success, error"
// @Security ApiKeyAuth
// @Router /makeRevalue [post]
func makeRevalueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RevalueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "invalid_request"})
		return
	}

	if req.UID == "" || req.Amount <= 0 || req.MachineID == "" || req.SessionID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "missing_fields"})
		return
	}

	if err := ensureUser(req.UID); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "internal_error"})
		return
	}

	productKey := "revalue:" + req.SessionID
	var existing TransactionModel
	findResult := db.Where("uid = ? AND product = ?", req.UID, productKey).First(&existing)

	if findResult.Error == nil {
		// Merge into existing transaction for this session
		if err := db.Model(&existing).UpdateColumn("amount", existing.Amount+req.Amount).Error; err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "internal_error"})
			return
		}
	} else if findResult.Error == gorm.ErrRecordNotFound {
		transaction := TransactionModel{
			UID:           &req.UID,
			Amount:        req.Amount,
			Product:       productKey,
			Status:        "confirmed",
			PaymentMethod: "cash",
			MachineID:     req.MachineID,
			IsCash:        true,
		}
		if err := db.Create(&transaction).Error; err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "internal_error"})
			return
		}
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "internal_error"})
		return
	}

	// Get updated balance
	var balanceResult struct {
		Balance int
	}
	db.Model(&TransactionModel{}).
		Select("COALESCE(SUM(amount), 0) as balance").
		Where("uid = ? AND status = ?", req.UID, "confirmed").
		Scan(&balanceResult)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":     true,
		"new_balance": balanceResult.Balance,
	})
}

// @Summary Record a cash purchase
// @Tags Purchases
// @Accept json
// @Produce json
// @Param body body CashPurchase true "Cash purchase details"
// @Success 201 {object} map[string]interface{} "transaction_id"
// @Failure 400 {string} string "Invalid purchase data"
// @Security ApiKeyAuth
// @Router /makeCashPurchase [post]
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
		Amount:        -purchase.Amount,
		Status:        "confirmed",
		Product:       get_product_name(purchase.Product),
		MachineID:     purchase.MachineID,
		IsCash:        true,
		PaymentMethod: "cash",
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

// @Summary Create a pending digital purchase
// @Description Creates a pending purchase that must be confirmed within 60 seconds. Checks for free vend privileges and vouchers before charging balance.
// @Tags Purchases
// @Accept json
// @Produce json
// @Param body body PurchaseRequest true "Purchase details"
// @Success 200 {object} map[string]interface{} "transaction_id"
// @Failure 400 {string} string "Missing fields"
// @Failure 403 {string} string "Insufficient balance"
// @Security ApiKeyAuth
// @Router /makePurchase [post]
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

// @Summary Confirm a pending purchase
// @Tags Purchases
// @Accept json
// @Param body body ConfirmRequest true "Transaction ID to confirm"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Bad request"
// @Failure 404 {string} string "Transaction not found or already processed"
// @Security ApiKeyAuth
// @Router /confirmPurchase [post]
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

// @Summary Get transaction history
// @Description Returns transactions with optional UID filter and pagination. Default limit 100, max 1000.
// @Tags Transactions
// @Accept json
// @Produce json
// @Param body body TransactionsRequest true "Filter and pagination options"
// @Success 200 {array} Transaction
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /getTransactions [post]
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

// @Summary Edit a transaction
// @Tags Transactions
// @Accept json
// @Param body body EditTransactionRequest true "Updated transaction fields"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Invalid request"
// @Failure 404 {string} string "Transaction not found"
// @Security ApiKeyAuth
// @Router /editTransaction [post]
func editTransactionHandler(w http.ResponseWriter, r *http.Request) {
	var req EditTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	validStatuses := map[string]bool{"pending": true, "confirmed": true, "failed": true}
	if !validStatuses[req.Status] {
		http.Error(w, "Invalid status (must be pending, confirmed, or failed)", http.StatusBadRequest)
		return
	}

	var tx TransactionModel
	if err := db.First(&tx, req.ID).Error; err != nil {
		http.Error(w, "Transaction not found", http.StatusNotFound)
		return
	}

	tx.UID = req.UID
	tx.Amount = req.Amount
	tx.Product = req.Product
	tx.Status = req.Status
	tx.PaymentMethod = req.PaymentMethod
	tx.MachineID = req.MachineID

	if err := db.Save(&tx).Error; err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// @Summary Delete a transaction
// @Tags Transactions
// @Accept json
// @Param body body DeleteByIDRequest true "Transaction ID to delete"
// @Success 200 {string} string "OK"
// @Failure 404 {string} string "Transaction not found"
// @Security ApiKeyAuth
// @Router /deleteTransaction [post]
func deleteTransactionHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteByIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result := db.Where("id = ?", req.ID).Delete(&TransactionModel{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "Transaction not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
