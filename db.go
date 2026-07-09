package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func generateAPIKey() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		log.Fatal("Failed to generate API key:", err)
	}
	return hex.EncodeToString(bytes)
}

func initDB() error {
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDB := os.Getenv("PG_DBNAME")
	pgHost := os.Getenv("PG_HOST")

	if pgUser == "" || pgPassword == "" || pgDB == "" || pgHost == "" {
		return fmt.Errorf("Missing one or more PostgreSQL environment variables")
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", pgHost, pgUser, pgPassword, pgDB)

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return err
	}

	// Auto-migrate the schema
	err = db.AutoMigrate(
		&User{},
		&UserMachinePrivilege{},
		&VendVoucher{},
		&TransactionModel{},
		&APIKey{},
		&ProductMap{},
		&Session{},
		&Firmware{},
	)
	if err != nil {
		return err
	}

	// Create indexes manually (GORM doesn't handle composite indexes in AutoMigrate well)
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_uid ON transactions(uid)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_transactions_metrics ON transactions(status, amount, product, machine_id, is_cash, payment_method)")
	db.Exec("CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)")

	return nil
}

func ensureUser(uid string) error {
	user := User{UID: uid}
	// FirstOrCreate will insert if not exists
	return db.Where(User{UID: uid}).FirstOrCreate(&user).Error
}
