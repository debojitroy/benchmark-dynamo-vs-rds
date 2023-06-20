package models

import (
	"database/sql"
	"errors"
	mysqlErrors "github.com/go-mysql/errors"
	"log"
	"time"
)

const maxRetries = 3

const insertNewRecordStatement string = `
	INSERT INTO tbl_orders (order_id, merchant_id, amount, currency, status, created_at, updated_at) 
	VALUES (?, ?, ?, ?, ?, ?, ?)
`

const updateRecordStatement string = `
	UPDATE tbl_orders SET amount = ?, currency = ?, status = ?, updated_at = ? where order_id = ?
`

type RdbmsInsertRecord struct {
	orderId    string
	merchantId string
	amount     float64
	currency   string
	status     string
	createdAt  int64
	updatedAt  int64
}

type RdbmsUpdateRecord struct {
	orderId   string
	amount    float64
	currency  string
	status    string
	updatedAt int64
}

func RdbmsInsert(recordForInsert *RdbmsInsertRecord, db *sql.DB) (bool, error) {
	// Prepare statement for inserting data
	stmtIns, insertStmtErr := db.Prepare(insertNewRecordStatement)
	if insertStmtErr != nil {
		log.Printf("Failed to create Insert Statement: %v\n", insertStmtErr)
		return false, insertStmtErr
	}
	defer func(stmtIns *sql.Stmt) {
		err := stmtIns.Close()
		if err != nil {
			log.Printf("Failed to close Open Statements")
		}
	}(stmtIns)

	for tryNo := 1; tryNo <= maxRetries; tryNo++ {
		log.Println("Trying to insert record")

		// Try Insert
		_, insertErr := stmtIns.Exec(recordForInsert.orderId,
			recordForInsert.merchantId,
			recordForInsert.amount,
			recordForInsert.currency,
			recordForInsert.status,
			time.Unix(recordForInsert.createdAt, 0).UTC().Format("2006-01-02 15:04:05"),
			time.Unix(recordForInsert.updatedAt, 0).UTC().Format("2006-01-02 15:04:05"))

		if insertErr != nil {
			if ok, sqlError := mysqlErrors.Error(insertErr); ok { // MySql error
				// Check if retry is possible
				if mysqlErrors.CanRetry(sqlError) {
					// Exponential Backup
					time.Sleep(time.Duration(tryNo*tryNo) * time.Second)
					continue
				}

				// Cannot proceed anymore
				return false, insertErr
			}
		}

		// Insert successful
		log.Println("Successfully inserted record")

		return true, nil
	}

	return false, errors.New("failed to process record as max number of retries exceeded")
}
