package main

import (
	"context"
	"database/sql"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/controllers"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/middlewares"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/services"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func getDBConnectionDetails() (services.DBConnection, error) {

	host, hostOk := os.LookupEnv("DB_HOST")

	if !hostOk {
		log.Fatal("Host Details are not available")
	}

	dbPort, dbPortOk := os.LookupEnv("DB_PORT")

	if !dbPortOk {
		log.Fatal("DB Port is not available")
	}

	port, portErr := strconv.ParseInt(dbPort, 10, 0)

	if portErr != nil {
		log.Fatal("DB Port is not valid Integer")
	}

	username, usernameOk := os.LookupEnv("DB_USERNAME")

	if !usernameOk {
		log.Fatal("Username is not available")
	}

	password, passwordOk := os.LookupEnv("DB_PASSWORD")

	if !passwordOk {
		log.Fatal("Username is not available")
	}

	driver, driverOk := os.LookupEnv("DB_DRIVER")

	if !driverOk {
		log.Fatal("Driver is not available")
	}

	schema, schemaOk := os.LookupEnv("DB_SCHEMA")

	if !schemaOk {
		log.Fatal("DB Schema is not available")
	}

	dbMaxConnLifeTime, dbMaxConnLifeTimeOk := os.LookupEnv("DB_CONN_MAX_LIFE_MIN")

	if !dbMaxConnLifeTimeOk {
		log.Fatal("DB Connection Max Lifetime is not available")
	}

	dbMaxConnLifetimeParsed, lifetimeErr := strconv.ParseInt(dbMaxConnLifeTime, 10, 0)

	if lifetimeErr != nil {
		log.Fatal("DB Connection Lifetime is not valid Integer")
	}

	dbPoolSize, dbPoolSizeOk := os.LookupEnv("DB_CONN_POOL_SIZE")

	if !dbPoolSizeOk {
		log.Fatal("DB Connection Pool Size is not available")
	}

	dbPoolSizeParsed, poolSizeErr := strconv.ParseInt(dbPoolSize, 10, 0)

	if poolSizeErr != nil {
		log.Fatal("DB Connection Pool Size is not valid Integer")
	}

	return services.DBConnection{
		Hostname:                 host,
		Port:                     int(port),
		Username:                 username,
		Password:                 password,
		Driver:                   driver,
		Database:                 schema,
		ConnMaxLifetimeInMinutes: dbMaxConnLifetimeParsed,
		MaxOpenConns:             int(dbPoolSizeParsed),
		MaxIdleConns:             int(dbPoolSizeParsed),
	}, nil
}

func setupRouter(dbConnection *sql.DB) *gin.Engine {
	r := gin.Default()
	r.Use(middlewares.Timer())

	r.POST("/v1/rdbms/orders", func(c *gin.Context) {

		var orderRequest controllers.OrderCreateRequest

		if c.Bind(&orderRequest) == nil {
			orderResponse, err := controllers.CreateOrder(&orderRequest, dbConnection)

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			} else {
				c.JSON(http.StatusOK, orderResponse)
			}
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "order body is missing"})
		}
	})

	r.GET("/v1/rdbms/orders", func(c *gin.Context) {
		orderId := c.Query("order_id")

		if orderId != "" {
			orderDetailsResponse, err := controllers.SelectOrder(orderId, dbConnection)

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			} else {
				c.JSON(http.StatusOK, orderDetailsResponse)
			}
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "order_id is missing"})
		}
	})

	r.POST("/v1/ddb/orders", func(c *gin.Context) {

		config := services.ConfigureAws()

		var orderRequest controllers.OrderCreateRequest

		if c.Bind(&orderRequest) == nil {
			orderResponse, err := controllers.CreateDynamoDbOrder(&config, &orderRequest)

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			} else {
				c.JSON(http.StatusOK, orderResponse)
			}
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "order body is missing"})
		}
	})

	r.GET("/v1/ddb/orders", func(c *gin.Context) {
		config := services.ConfigureAws()

		orderId := c.Query("order_id")

		if orderId != "" {
			orderDetailsResponse, err := controllers.SelectDynamoDbOrder(&config, orderId)

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			} else {
				c.JSON(http.StatusOK, orderDetailsResponse)
			}
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "order_id is missing"})
		}
	})

	return r
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

	r := setupRouter(dbConnection)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		log.Println("Starting Server...")
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
