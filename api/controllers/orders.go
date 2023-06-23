package controllers

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	dynamoModel "github.com/debojitroy/benchmark-dynamo-vs-rds/api/models/dynamodb"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/models/rdbms"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/utils"
	"log"
	"time"
)

type OrderCreateRequest struct {
	MerchantId string  `json:"merchant_id" binding:"required"`
	Amount     float64 `json:"amount" binding:"required"`
	Currency   string  `json:"currency" binding:"required"`
}

type OrderCreateResponse struct {
	OrderId string `json:"order_id" binding:"required"`
}

type OrderSelectResponse struct {
	OrderId    string    `json:"order_id" binding:"required"`
	MerchantId string    `json:"merchant_id" binding:"required"`
	Amount     float64   `json:"amount" binding:"required"`
	Currency   string    `json:"currency" binding:"required"`
	Status     string    `json:"status" binding:"required"`
	CreatedAt  time.Time `json:"created_at" binding:"required"`
	UpdatedAt  time.Time `json:"updated_at" binding:"required"`
}

func CreateOrder(order *OrderCreateRequest, db *sql.DB) (OrderCreateResponse, error) {
	orderId := utils.GenerateId()

	orderInput := models.RdbmsInsertRecord{
		OrderId:    orderId,
		MerchantId: order.MerchantId,
		Amount:     order.Amount,
		Currency:   order.Currency,
		Status:     "NEW",
		CreatedAt:  time.Now().Unix(),
		UpdatedAt:  time.Now().Unix(),
	}

	_, err := models.RdbmsInsert(&orderInput, db)

	if err != nil {
		fmt.Printf("Failed to create order %v", err)
		return OrderCreateResponse{OrderId: ""}, err
	}

	return OrderCreateResponse{OrderId: orderId}, nil
}

func SelectOrder(orderId string, db *sql.DB) (OrderSelectResponse, error) {
	orderRecord, err := models.RdbmsSelect(orderId, db)

	if err != nil {
		log.Printf("Failed to select order %v", err)
		return OrderSelectResponse{OrderId: ""}, err
	}

	return OrderSelectResponse{OrderId: orderRecord.OrderId, MerchantId: orderRecord.MerchantId, Amount: orderRecord.Amount, Currency: orderRecord.Currency, Status: orderRecord.Status, CreatedAt: orderRecord.CreatedAt, UpdatedAt: orderRecord.UpdatedAt}, nil
}

func CreateDynamoDbOrder(config *aws.Config, order *OrderCreateRequest) (OrderCreateResponse, error) {
	request := dynamoModel.DynamoDbNewOrderInput(order.MerchantId, order.Amount, order.Currency, "NEW")

	orderId, err := dynamoModel.DynamoDbCreateOrder(config, request)

	if err != nil {
		log.Printf("Failed to create order %v", err)
		return OrderCreateResponse{OrderId: ""}, err
	}

	return OrderCreateResponse{OrderId: orderId}, nil
}

func SelectDynamoDbOrder(config *aws.Config, orderId string) (OrderSelectResponse, error) {
	orderRecord, err := dynamoModel.DynamoDbSelectOrder(config, orderId)

	if err != nil {
		log.Printf("Failed to select order %v", err)
		return OrderSelectResponse{}, err
	}

	return OrderSelectResponse{
		OrderId:    orderRecord.OrderId,
		MerchantId: orderRecord.MerchantId,
		Amount:     orderRecord.Amount,
		Currency:   orderRecord.Currency,
		Status:     orderRecord.Status,
		CreatedAt:  time.Unix(orderRecord.CreatedAt, 0),
		UpdatedAt:  time.Unix(orderRecord.UpdatedAt, 0),
	}, nil
}
