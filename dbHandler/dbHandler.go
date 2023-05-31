package dbHandler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"time"
)

type TableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func (basics TableBasics) CreateTable() (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	table, err := basics.DynamoDbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Address"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("Address"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(basics.TableName),
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", basics.TableName, err)
	} else {
		fmt.Printf("table created successfully: %s\n", *table.TableDescription.TableName)
		waiter := dynamodb.NewTableExistsWaiter(basics.DynamoDbClient)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(basics.TableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
		tableDesc = table.TableDescription
	}
	return tableDesc, err
}

func (basics TableBasics) AddBuilding(building map[string]string) error {
	item, err := attributevalue.MarshalMap(building)
	fmt.Println(building)
	if err != nil {
		panic(err)
	}

	_, err = basics.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(basics.TableName), Item: item,
	})

	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
	}

	return err
}

func (basics TableBasics) AddBuildings(buildings []map[string]string) error {
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
	_, err = basics.DynamoDbClient.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{basics.TableName: writeReqs},
	})
	if err != nil {
		log.Printf("Couldn't add a batch of buildings to %v. Here's why: %v\n", basics.TableName, err)
	}

	return nil
}
