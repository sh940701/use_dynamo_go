package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"use_dynamo_go/dbHandler"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	basic := dbHandler.TableBasics{
		DynamoDbClient: client,
		TableName:      "buildings",
	}

	tableDesc, err := basic.CreateTable()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("tableDesc: ", tableDesc, "\n", "err: ", err)

}
