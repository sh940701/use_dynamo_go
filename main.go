package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"os"
	"time"
	"use_dynamo_go/dataHandler"
	"use_dynamo_go/dbHandler"
)

func main() {
	start := time.Now()

	// 기본 세팅
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	basic := dbHandler.TableBasics{
		DynamoDbClient: client,
		TableName:      "buildings",
	}

	// 테이블 생성
	CreateTable(basic)

	inputFilePath := "testForEach.txt"
	//inputFilePath := "testForBatch.txt"
	//inputFilePath := "/Users/sunghyun/Desktop/mart_djy_03.txt"

	// 개별 데이터 insert
	InsertDataEach(inputFilePath, basic)

	// 25개 단위 데이터 insert
	InsertDataBatch(inputFilePath, basic)

	duration := time.Since(start)

	fmt.Println("경과 시간: ", duration)
}

// 테이블 생성 함수
func CreateTable(basic dbHandler.TableBasics) {
	//테이블 생성
	tableDesc, err := basic.CreateTable()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("tableDesc: ", tableDesc, "\n", "err: ", err)
}

// 데이터 한개씩 삽입 함수
func InsertDataEach(inputFilePath string, basic dbHandler.TableBasics) {
	// file config
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

// 데이터 25개씩 삽입 함수
func InsertDataBatch(inputFilePath string, basic dbHandler.TableBasics) {
	// file config
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	MAX_COUNT := 25

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
		if len(buildings) == MAX_COUNT {
			buildingsCopy := make([]map[string]string, MAX_COUNT)
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
