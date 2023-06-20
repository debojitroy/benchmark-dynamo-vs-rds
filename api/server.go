package main

import (
	"context"
	"database/sql"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/middlewares"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/services"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func getDBConnectionDetails() (services.DBConnection, error) {
	return services.DBConnection{
		Hostname:                 "localhost",
		Port:                     3306,
		Username:                 "admin",
		Password:                 "password",
		Driver:                   "mysql",
		Database:                 "pgrouter",
		ConnMaxLifetimeInMinutes: 3,
		MaxOpenConns:             10,
		MaxIdleConns:             10,
	}, nil
}

func main() {
	gin.ForceConsoleColor()

	connectionDetails, _ := getDBConnectionDetails()

	dbConnection, connectionErr := services.GetDbConnection(connectionDetails)

	if connectionErr != nil {
		log.Fatal(connectionErr)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println("Failed to close DB Connection...")
		} else {
			log.Println("Successfully closed DB Connection....")
		}
	}(dbConnection)

	r := gin.Default()

	r.Use(middlewares.Timer())

	r.GET("/ping", func(c *gin.Context) {
		// Execute the query
		results, err := dbConnection.Query("select now() as time")
		if err != nil {
			log.Fatalf("Failed to query Database: %v", err)
		}

		var currentTime time.Time

		for results.Next() {
			// for each row, scan the result into our tag composite object
			err = results.Scan(&currentTime)
			if err != nil {
				log.Fatalf("Failed to process Results: %v", err)
			}

			// and then print out the tag's Name attribute
			log.Printf("Time now: %v", currentTime)
		}

		c.JSON(200, gin.H{
			"message":     "pong",
			"currentTime": currentTime,
		})
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Wait for interrupt signal to gracefully shut down the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	log.Println("Server exiting")
}
