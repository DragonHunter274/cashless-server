package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// @Summary List all API keys (masked)
// @Tags API Keys
// @Produce json
// @Success 200 {array} object "List of masked API keys"
// @Security ApiKeyAuth
// @Router /getAPIKeys [post]
func getAPIKeysHandler(w http.ResponseWriter, r *http.Request) {
	var keys []APIKey
	db.Order("created_at DESC").Find(&keys)

	type MaskedKey struct {
		Key              string    `json:"key"`
		AllowedEndpoints string    `json:"allowed_endpoints"`
		CreatedAt        time.Time `json:"created_at"`
	}

	var masked []MaskedKey
	for _, k := range keys {
		maskedKey := k.Key
		if len(k.Key) > 8 {
			maskedKey = k.Key[:4] + "..." + k.Key[len(k.Key)-4:]
		}
		masked = append(masked, MaskedKey{
			Key:              maskedKey,
			AllowedEndpoints: k.AllowedEndpoints,
			CreatedAt:        k.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(masked)
}

// @Summary Create a new API key
// @Tags API Keys
// @Accept json
// @Produce json
// @Param body body CreateAPIKeyRequest true "Allowed endpoints (comma-separated)"
// @Success 201 {object} APIKey
// @Failure 400 {string} string "allowed_endpoints is required"
// @Security ApiKeyAuth
// @Router /createAPIKey [post]
func createAPIKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.AllowedEndpoints == "" {
		http.Error(w, "allowed_endpoints is required", http.StatusBadRequest)
		return
	}

	key := APIKey{
		Key:              generateAPIKey(),
		AllowedEndpoints: req.AllowedEndpoints,
	}

	if err := db.Create(&key).Error; err != nil {
		http.Error(w, "Failed to create API key", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(key)
}

// @Summary Delete an API key
// @Tags API Keys
// @Accept json
// @Param body body DeleteAPIKeyRequest true "API key to delete"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Cannot delete the API key currently in use"
// @Failure 404 {string} string "API key not found"
// @Security ApiKeyAuth
// @Router /deleteAPIKey [post]
func deleteAPIKeyHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Prevent self-deletion
	currentKey := r.Header.Get("X-API-Key")
	if req.Key == currentKey {
		http.Error(w, "Cannot delete the API key currently in use", http.StatusBadRequest)
		return
	}

	result := db.Where("key = ?", req.Key).Delete(&APIKey{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "API key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// @Summary List all product mappings
// @Tags Products
// @Produce json
// @Success 200 {array} ProductMap
// @Security ApiKeyAuth
// @Router /getProductMap [post]
func getProductMapHandler(w http.ResponseWriter, r *http.Request) {
	var products []ProductMap
	db.Order("id ASC").Find(&products)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

// @Summary Create or update a product mapping
// @Tags Products
// @Accept json
// @Param body body ProductMapRequest true "Product ID and name"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "id and product_name are required"
// @Security ApiKeyAuth
// @Router /createProductMapping [post]
func createProductMappingHandler(w http.ResponseWriter, r *http.Request) {
	var req ProductMapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if req.ID <= 0 || req.ProductName == "" {
		http.Error(w, "id and product_name are required", http.StatusBadRequest)
		return
	}

	product := ProductMap{ID: req.ID, ProductName: req.ProductName}
	if err := db.Save(&product).Error; err != nil {
		http.Error(w, "Failed to save product mapping", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// @Summary Delete a product mapping
// @Tags Products
// @Accept json
// @Param body body DeleteByIDRequest true "Product ID to delete"
// @Success 200 {string} string "OK"
// @Failure 404 {string} string "Product mapping not found"
// @Security ApiKeyAuth
// @Router /deleteProductMapping [post]
func deleteProductMappingHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteByIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result := db.Where("id = ?", req.ID).Delete(&ProductMap{})
	if result.Error != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected == 0 {
		http.Error(w, "Product mapping not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
