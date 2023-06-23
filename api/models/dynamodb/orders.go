package models

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/services"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/utils"
	"log"
	"time"
)

const recordTtlInHours = 3 * 24 // 3 days

const order = "ORDER"

type DynamoDbOrderInput struct {
	merchantId string
	amount     float64
	currency   string
	status     string
}

type DynamoDbOrderRecord struct {
	OrderId    string  `dynamodbav:"p_key" json:"order_id"`
	RecordType string  `dynamodbav:"s_key" json:"record_type"`
	MerchantId string  `dynamodbav:"merchant_id" json:"merchant_id"`
	Amount     float64 `dynamodbav:"amount" json:"amount"`
	Currency   string  `dynamodbav:"currency" json:"currency"`
	Status     string  `dynamodbav:"status" json:"status"`
	CreatedAt  int64   `dynamodbav:"created_at" json:"created_at"`
	UpdatedAt  int64   `dynamodbav:"updated_at" json:"updated_at"`
	RecordTtl  int64   `dynamodbav:"record_ttl" json:"record_ttl"`
}

func DynamoDbNewOrderInput(merchantId string, amount float64, currency string, status string) *DynamoDbOrderInput {
	order := DynamoDbOrderInput{merchantId: merchantId, amount: amount, currency: currency, status: status}
	return &order
}

func DynamoDbCreateOrder(config *aws.Config, input *DynamoDbOrderInput) (string, error) {
	id := utils.GenerateId()

	item := make(map[string]types.AttributeValue)

	item["p_key"] = &types.AttributeValueMemberS{Value: id}
	item["s_key"] = &types.AttributeValueMemberS{Value: order}
	item["merchant_id"] = &types.AttributeValueMemberS{Value: input.merchantId}
	item["amount"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", input.amount)}
	item["currency"] = &types.AttributeValueMemberS{Value: input.currency}
	item["status"] = &types.AttributeValueMemberS{Value: input.status}
	item["created_at"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())}
	item["updated_at"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())}
	item["record_ttl"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Add(time.Duration(recordTtlInHours)*time.Hour).Unix())}

	resp, err := services.PutItemInDynamoDB(config, &item)

	if err != nil {
		log.Printf("Failed to create order, %v", err)
		return "", err
	}

	log.Println("Order Created...")
	for name, value := range resp.Attributes {
		log.Printf("Name: %s , Value: %s | ", name, value)
	}

	return id, nil
}

func DynamoDbSelectOrder(config *aws.Config, orderId string) (*DynamoDbOrderRecord, error) {
	item := make(map[string]types.AttributeValue)

	item["p_key"] = &types.AttributeValueMemberS{Value: orderId}
	item["s_key"] = &types.AttributeValueMemberS{Value: order}

	resp, err := services.GetItemFromDynamoDB(config, &item)

	if err != nil {
		log.Printf("Failed to get order item, %v", err)
		return nil, err
	}

	if resp.Item == nil {
		log.Printf("Item not found, %v", err)
		return nil, errors.New("order not found")
	}

	orderDetails := DynamoDbOrderRecord{}

	err = attributevalue.UnmarshalMap(resp.Item, &orderDetails)
	if err != nil {
		log.Println("unmarshal failed with error", err)
		return nil, err
	}

	return &orderDetails, nil
}
