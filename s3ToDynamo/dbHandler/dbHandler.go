package dbHandler

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
)

type TableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func AddBuildings(buildings []map[string]interface{}, client *dynamodb.Client, tableName string) error {
	var err error
	var item map[string]types.AttributeValue
	var writeReqs []types.WriteRequest
	for _, building := range buildings {
		item, err = attributevalue.MarshalMap(building)
		if err != nil {
			log.Printf("Couldn't marshal building %v for batch writing. Here's why: %v\n", building, err)
		} else {
			writeReqs = append(
				writeReqs,
				types.WriteRequest{PutRequest: &types.PutRequest{Item: item}},
			)
		}
	}
	_, err = client.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{tableName: writeReqs},
	})
	if err != nil {
		log.Printf("Couldn't add a batch of buildings to %v. Here's why: %v\n", tableName, err)
	}
	return err
}
