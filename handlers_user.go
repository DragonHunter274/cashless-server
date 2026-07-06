package main

import (
	"encoding/json"
	"net/http"
)

// @Summary Get user balance
// @Tags Users
// @Accept json
// @Produce json
// @Param body body BalanceRequest true "User UID"
// @Success 200 {object} Balance
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /getBalance [post]
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

// @Summary Create a new user
// @Tags Users
// @Accept json
// @Param body body UserRequest true "User UID"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /createUser [post]
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

// @Summary Get dashboard statistics
// @Tags Stats
// @Produce json
// @Success 200 {object} StatsResponse
// @Security ApiKeyAuth
// @Router /getStats [post]
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

// @Summary Get paginated user list with balances
// @Tags Users
// @Accept json
// @Produce json
// @Param body body GetUsersRequest true "Pagination and search options"
// @Success 200 {object} GetUsersResponse
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /getUsers [post]
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
