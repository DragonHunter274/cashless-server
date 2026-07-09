package main

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"

	"gorm.io/gorm"
)

// firmwareSignatureSize is the size in bytes of the RSA signature embedded at
// the start of a signed firmware.img, per spec/ota.md:
// [512-byte RSA signature][firmware binary]
const firmwareSignatureSize = 512

// firmwareMaxUploadSize caps the accepted multipart upload size.
const firmwareMaxUploadSize = 16 << 20 // 16 MB

var firmwareVersionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

var (
	firmwarePubKeyOnce sync.Once
	firmwarePubKey     *rsa.PublicKey
)

// getFirmwarePublicKey loads the optional RSA public key used to verify
// uploaded firmware signatures. Configured via FIRMWARE_RSA_PUBLIC_KEY_PATH,
// pointing at the same rsa_key.pub embedded in device firmware (see
// spec/ota.md). If unset, upload-time verification is skipped - devices
// still verify the signature themselves before flashing.
func getFirmwarePublicKey() *rsa.PublicKey {
	firmwarePubKeyOnce.Do(func() {
		path := os.Getenv("FIRMWARE_RSA_PUBLIC_KEY_PATH")
		if path == "" {
			return
		}
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("WARNING: failed to read FIRMWARE_RSA_PUBLIC_KEY_PATH: %v (upload signature verification disabled)", err)
			return
		}
		block, _ := pem.Decode(data)
		if block == nil {
			log.Printf("WARNING: FIRMWARE_RSA_PUBLIC_KEY_PATH does not contain a valid PEM block (upload signature verification disabled)")
			return
		}
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			log.Printf("WARNING: failed to parse firmware RSA public key: %v (upload signature verification disabled)", err)
			return
		}
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			log.Printf("WARNING: firmware public key is not an RSA key (upload signature verification disabled)")
			return
		}
		firmwarePubKey = rsaPub
		log.Println("Firmware upload signature verification enabled")
	})
	return firmwarePubKey
}

// verifyFirmwareSignature checks the embedded RSA signature of a signed
// firmware.img against the configured public key. If no public key is
// configured, verification is skipped (nil error).
func verifyFirmwareSignature(data []byte) error {
	pub := getFirmwarePublicKey()
	if pub == nil {
		return nil
	}
	signature := data[:firmwareSignatureSize]
	firmware := data[firmwareSignatureSize:]
	hash := sha256.Sum256(firmware)
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, hash[:], signature)
}

// @Summary Upload a signed firmware image
// @Description Uploads an RSA-signed firmware.img (512-byte embedded signature + binary) for a given version. Does not activate it - use /activateFirmware to publish it to devices.
// @Tags Firmware
// @Accept mpfd
// @Produce json
// @Param version formData string true "Semantic version, e.g. 1.0.1"
// @Param firmware formData file true "Signed firmware.img"
// @Success 201 {object} Firmware
// @Failure 400 {string} string "Invalid request"
// @Failure 409 {string} string "Version already exists"
// @Security ApiKeyAuth
// @Router /uploadFirmware [post]
func uploadFirmwareHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, firmwareMaxUploadSize)

	if err := r.ParseMultipartForm(firmwareMaxUploadSize); err != nil {
		http.Error(w, "Invalid multipart upload (file too large or malformed)", http.StatusBadRequest)
		return
	}

	version := r.FormValue("version")
	if !firmwareVersionPattern.MatchString(version) {
		http.Error(w, "version is required and must be a semantic version, e.g. 1.0.1", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("firmware")
	if err != nil {
		http.Error(w, "firmware file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "Failed to read uploaded file", http.StatusBadRequest)
		return
	}

	if len(data) <= firmwareSignatureSize {
		http.Error(w, "Firmware file too small to contain an embedded signature", http.StatusBadRequest)
		return
	}

	if err := verifyFirmwareSignature(data); err != nil {
		http.Error(w, "Firmware signature verification failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	var existing Firmware
	if err := db.Where("version = ?", version).First(&existing).Error; err == nil {
		http.Error(w, "Version already exists", http.StatusConflict)
		return
	}

	firmware := Firmware{
		Version:  version,
		Filename: header.Filename,
		Data:     data,
		Size:     len(data),
	}

	if err := db.Create(&firmware).Error; err != nil {
		http.Error(w, "Failed to save firmware", http.StatusInternalServerError)
		return
	}

	firmware.Data = nil
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(firmware)
}

// @Summary List uploaded firmware versions
// @Tags Firmware
// @Produce json
// @Success 200 {array} Firmware
// @Security ApiKeyAuth
// @Router /getFirmwareList [post]
func getFirmwareListHandler(w http.ResponseWriter, r *http.Request) {
	var firmware []Firmware
	db.Omit("data").Order("created_at DESC").Find(&firmware)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(firmware)
}

// @Summary Activate a firmware version
// @Description Publishes the given version as the active firmware served at /firmware/manifest.json and /firmware/firmware.img.
// @Tags Firmware
// @Accept json
// @Param body body FirmwareVersionRequest true "Version to activate"
// @Success 200 {string} string "OK"
// @Failure 404 {string} string "Firmware version not found"
// @Security ApiKeyAuth
// @Router /activateFirmware [post]
func activateFirmwareHandler(w http.ResponseWriter, r *http.Request) {
	var req FirmwareVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Version == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var firmware Firmware
	if err := db.Where("version = ?", req.Version).First(&firmware).Error; err != nil {
		http.Error(w, "Firmware version not found", http.StatusNotFound)
		return
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&Firmware{}).Where("active = ?", true).Update("active", false).Error; err != nil {
			return err
		}
		return tx.Model(&Firmware{}).Where("version = ?", req.Version).Update("active", true).Error
	})
	if err != nil {
		http.Error(w, "Failed to activate firmware", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// @Summary Delete an uploaded firmware version
// @Tags Firmware
// @Accept json
// @Param body body FirmwareVersionRequest true "Version to delete"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Cannot delete the active firmware version"
// @Failure 404 {string} string "Firmware version not found"
// @Security ApiKeyAuth
// @Router /deleteFirmware [post]
func deleteFirmwareHandler(w http.ResponseWriter, r *http.Request) {
	var req FirmwareVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Version == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var firmware Firmware
	if err := db.Where("version = ?", req.Version).First(&firmware).Error; err != nil {
		http.Error(w, "Firmware version not found", http.StatusNotFound)
		return
	}

	if firmware.Active {
		http.Error(w, "Cannot delete the active firmware version", http.StatusBadRequest)
		return
	}

	if err := db.Delete(&firmware).Error; err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// @Summary OTA manifest for the active firmware
// @Description Public endpoint consumed by devices (esp32FOTA) to check for updates. No authentication required.
// @Tags Firmware
// @Produce json
// @Success 200 {object} object "esp32-fota-http manifest"
// @Failure 404 {string} string "No active firmware"
// @Router /firmware/manifest.json [get]
func firmwareManifestHandler(w http.ResponseWriter, r *http.Request) {
	var firmware Firmware
	if err := db.Omit("data").Where("active = ?", true).First(&firmware).Error; err != nil {
		http.Error(w, "No active firmware", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"type":    "esp32-fota-http",
		"version": firmware.Version,
		"bin":     "firmware.img",
	})
}

// @Summary Download the active signed firmware image
// @Description Endpoint consumed by devices (esp32FOTA) to download the active firmware.img, referenced relative to the manifest. Requires an API key or OIDC session, so devices must be configured with an X-API-Key header.
// @Tags Firmware
// @Produce octet-stream
// @Success 200 {file} file "Signed firmware.img"
// @Failure 401 {string} string "Authentication required"
// @Failure 404 {string} string "No active firmware"
// @Security ApiKeyAuth
// @Router /firmware/firmware.img [get]
func firmwareBinaryHandler(w http.ResponseWriter, r *http.Request) {
	var firmware Firmware
	if err := db.Where("active = ?", true).First(&firmware).Error; err != nil {
		http.Error(w, "No active firmware", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=firmware.img")
	w.Write(firmware.Data)
}
