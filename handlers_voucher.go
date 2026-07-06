package main

import (
	"encoding/json"
	"net/http"
)

// @Summary Get vouchers
// @Description Returns vouchers with optional UID filter.
// @Tags Vouchers
// @Accept json
// @Produce json
// @Param body body UserRequest true "Optional UID filter"
// @Success 200 {array} VendVoucher
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /getVouchers [post]
func getVouchersHandler(w http.ResponseWriter, r *http.Request) {
	var req UserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	query := db.Model(&VendVoucher{})

	// Filter by UID if provided
	if req.UID != "" {
		query = query.Where("uid = ?", req.UID)
	}

	var vouchers []VendVoucher
	if err := query.Order("created_at DESC").Find(&vouchers).Error; err != nil {
		http.Error(w, "Error fetching vouchers: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(vouchers)
}

// @Summary Get machine privileges
// @Description Returns machine privileges with optional UID filter.
// @Tags Privileges
// @Accept json
// @Produce json
// @Param body body UserRequest true "Optional UID filter"
// @Success 200 {array} UserMachinePrivilege
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /getPrivileges [post]
func getPrivilegesHandler(w http.ResponseWriter, r *http.Request) {
	var req UserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	query := db.Model(&UserMachinePrivilege{})

	// Filter by UID if provided
	if req.UID != "" {
		query = query.Where("uid = ?", req.UID)
	}

	var privileges []UserMachinePrivilege
	if err := query.Find(&privileges).Error; err != nil {
		http.Error(w, "Error fetching privileges: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(privileges)
}

// @Summary Create a voucher
// @Tags Vouchers
// @Accept json
// @Param body body VoucherRequest true "Voucher details"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /createVoucher [post]
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

	voucher := VendVoucher{
		UID:       req.UID,
		MachineID: req.MachineID,
	}

	if err := db.Create(&voucher).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// @Summary Create or update a machine privilege
// @Tags Privileges
// @Accept json
// @Param body body PrivilegeRequest true "Privilege details"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Invalid request"
// @Security ApiKeyAuth
// @Router /createPrivilege [post]
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

	privilege := UserMachinePrivilege{
		UID:       req.UID,
		MachineID: req.MachineID,
		FreeVend:  req.FreeVend,
	}

	// Use Clauses to handle ON CONFLICT
	if err := db.Save(&privilege).Error; err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// @Summary Delete an unused voucher
// @Tags Vouchers
// @Accept json
// @Param body body DeleteByIDRequest true "Voucher ID to delete"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Cannot delete used voucher"
// @Failure 404 {string} string "Voucher not found"
// @Security ApiKeyAuth
// @Router /deleteVoucher [post]
func deleteVoucherHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteByIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var voucher VendVoucher
	if err := db.First(&voucher, req.ID).Error; err != nil {
		http.Error(w, "Voucher not found", http.StatusNotFound)
		return
	}

	if voucher.Used {
		http.Error(w, "Cannot delete used voucher", http.StatusBadRequest)
		return
	}

	db.Delete(&voucher)
	w.WriteHeader(http.StatusOK)
}

// @Summary Delete a machine privilege
// @Tags Privileges
// @Accept json
// @Param body body VoucherRequest true "UID and machine ID"
// @Success 200 {string} string "OK"
// @Failure 404 {string} string "Privilege not found"
// @Security ApiKeyAuth
// @Router /deletePrivilege [post]
func deletePrivilegeHandler(w http.ResponseWriter, r *http.Request) {
	var req VoucherRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UID == "" || req.MachineID == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result := db.Where("uid = ? AND machine_id = ?", req.UID, req.MachineID).Delete(&UserMachinePrivilege{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "Privilege not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
