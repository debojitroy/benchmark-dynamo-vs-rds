package services

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type DBConnection struct {
	Hostname                 string `json:"hostname"`
	Port                     int    `json:"port"`
	Username                 string `json:"username"`
	Password                 string `json:"password"`
	Database                 string `json:"db"`
	Driver                   string `json:"driver"`
	ConnMaxLifetimeInMinutes int64  `json:"conn_max_lifetime_in_minutes"`
	MaxOpenConns             int    `json:"max_open_conns"`
	MaxIdleConns             int    `json:"max_idle_conns"`
}

func GetDbConnection(dbConnectionDetails DBConnection) (*sql.DB, error) {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", dbConnectionDetails.Username, dbConnectionDetails.Password, dbConnectionDetails.Hostname, dbConnectionDetails.Port, dbConnectionDetails.Database)

	db, err := sql.Open(dbConnectionDetails.Driver,
		connectionString)

	if err != nil {
		fmt.Printf("Failed to open DB Connection, %v", err)
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * time.Duration(dbConnectionDetails.ConnMaxLifetimeInMinutes))
	db.SetMaxOpenConns(dbConnectionDetails.MaxOpenConns)
	db.SetMaxIdleConns(dbConnectionDetails.MaxIdleConns)

	return db, nil
}
