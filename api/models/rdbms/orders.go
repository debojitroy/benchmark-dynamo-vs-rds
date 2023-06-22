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
	INSERT INTO tbl_orders (order_id, merchant_id, Amount, Currency, status, created_at, updated_at) 
	VALUES (?, ?, ?, ?, ?, ?, ?)
`

const selectRecordStatement string = "SELECT order_id, merchant_id, amount, currency, status, created_at, updated_at from tbl_orders where order_id=?"

type RdbmsInsertRecord struct {
	OrderId    string
	MerchantId string
	Amount     float64
	Currency   string
	Status     string
	CreatedAt  int64
	UpdatedAt  int64
}

type RdbmsSelectRecord struct {
	OrderId    string
	MerchantId string
	Amount     float64
	Currency   string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
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
		_, insertErr := stmtIns.Exec(recordForInsert.OrderId,
			recordForInsert.MerchantId,
			recordForInsert.Amount,
			recordForInsert.Currency,
			recordForInsert.Status,
			time.Unix(recordForInsert.CreatedAt, 0).UTC().Format("2006-01-02 15:04:05"),
			time.Unix(recordForInsert.UpdatedAt, 0).UTC().Format("2006-01-02 15:04:05"))

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

func RdbmsSelect(orderId string, db *sql.DB) (*RdbmsSelectRecord, error) {
	// Prepare statement for inserting data
	stmtSelect, selectStmtErr := db.Prepare(selectRecordStatement)
	if selectStmtErr != nil {
		log.Printf("Failed to create Select Statement: %v\n", selectStmtErr)
		return nil, selectStmtErr
	}
	defer func(stmtSelect *sql.Stmt) {
		err := stmtSelect.Close()
		if err != nil {
			log.Printf("Failed to close Open Statements")
		}
	}(stmtSelect)

	log.Printf("Trying to search record for order Id: %s", orderId)

	// query
	rows, selectExecErr := stmtSelect.Query(orderId)
	if selectExecErr != nil {
		log.Printf("Failed to execute Select Statement: %v\n", selectExecErr)
		return nil, selectExecErr
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Printf("Failed to close Rows")
		}
	}(rows)

	selectRecord := RdbmsSelectRecord{}

	for rows.Next() {
		err := rows.Scan(&selectRecord.OrderId, &selectRecord.MerchantId, &selectRecord.Amount, &selectRecord.Currency, &selectRecord.Status, &selectRecord.CreatedAt, &selectRecord.UpdatedAt)
		if err != nil {
			log.Printf("Failed to map results from DB: %v\n", err)
			return nil, err
		}
	}

	resultsError := rows.Err()
	if resultsError != nil {
		log.Printf("Received Error from DB while scanning: %v\n", resultsError)
		return nil, resultsError
	}

	return &selectRecord, nil
}
