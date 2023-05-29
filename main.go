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

func InsertDataBatch(inputFilePath string, basic dbHandler.TableBasics) {
	// file config
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	// txt 파일의 모든 문서를 25개씩 write 요청
	// 1080개 기준 싱글스레드 경과 시간: 988.009375ms
	// 전체 파일 기준 싱글스레드 경과 시간: 2 시간 이상
	var buildings []map[string]string
	for scanner.Scan() {
		strData := dataHandler.SplitByPipeline(scanner.Text())
		building := map[string]string{
			"PK":      strData[0],
			"address": strData[6],
		}
		buildings = append(buildings, building)
		if len(buildings) == 25 {
			buildingsCopy := make([]map[string]string, len(buildings))
			copy(buildingsCopy, buildings)
			err := basic.AddBuildings(buildingsCopy)
			if err != nil {
				panic(err)
			}
			buildings = buildings[:0]
		}
	}

	// 남은 항목 추가
	err = basic.AddBuildings(buildings)
	if err != nil {
		panic(err)
	}
}
