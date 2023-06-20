package models

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/services"
	"github.com/debojitroy/benchmark-dynamo-vs-rds/api/utils"
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
		fmt.Printf("Failed to create order, %v", err)
		return "", err
	}

	fmt.Println("Order Created...")
	for name, value := range resp.Attributes {
		fmt.Printf("Name: %s , Value: %s | ", name, value)
	}

	return id, nil
}
