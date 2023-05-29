package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"os"
	"use_dynamo_go/dataHandler"
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

	//테이블 생성
	tableDesc, err := basic.CreateTable()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("tableDesc: ", tableDesc, "\n", "err: ", err)

	// file config
	inputFilePath := "testfile.txt"
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	// txt 파일의 모든 문서를 한개씩 write 요청
	for scanner.Scan() {
		strData := dataHandler.SplitByPipeline(scanner.Text())
		building := map[string]string{
			"PK":      strData[0],
			"address": strData[6],
		}
		err := basic.AddBuilding(building)
		if err != nil {
			panic(err)
		}
	}

}
