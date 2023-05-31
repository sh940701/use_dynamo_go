package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"os"
	"sync"
	"time"
	"use_dynamo_go/dataHandler"
	"use_dynamo_go/dbHandler"
)

func main() {
	start := time.Now()

	// 기본 세팅
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(so *retry.StandardOptions) {
			so.RateLimiter = ratelimit.NewTokenRateLimit(1000000)
		})
	}))
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

	//inputFilePath := "testForEach.txt"
	//inputFilePath := "testForBatch.txt"
	//inputFilePath := "/Users/sunghyun/Desktop/mart_djy_03.txt"

	// 개별 데이터 insert
	//InsertDataEach(inputFilePath, basic)

	// 25개 단위 데이터 insert
	//InsertDataBatch(inputFilePath, basic)

	// 25개 단위 병렬 데이터 insert
	//InsertDataBatchParallel(inputFilePath, basic)

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
		strData := dataHandler.SplitByString(scanner.Text(), "|")
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
		strData := dataHandler.SplitByString(scanner.Text(), "|")
		building := map[string]string{
			"PK":      strData[0],
			"address": strData[6],
		}
		buildings = append(buildings, building)
		if len(buildings) == MAX_COUNT {
			buildingsCopy := make([]map[string]string, MAX_COUNT)
			copy(buildingsCopy, buildings)
			basic.AddBuildings(buildingsCopy)
			buildings = buildings[:0]
		}
	}

	// 남은 항목 추가
	basic.AddBuildings(buildings)
}

// goroutine batch
func InsertDataBatchParallel(inputFilePath string, basic dbHandler.TableBasics) {
	counter := 0
	// file config
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer inputFile.Close()

	MAX_COUNT := 25

	scanner := bufio.NewScanner(inputFile)

	var buildings []map[string]string

	var wg sync.WaitGroup // WaitGroup 선언

	for scanner.Scan() {
		strData := dataHandler.SplitByString(scanner.Text(), "|")
		building := map[string]string{
			"PK":      strData[0],
			"address": strData[6],
		}
		buildings = append(buildings, building)

		if len(buildings) == MAX_COUNT*2300 {
			counter++
			wg.Add(1) // WaitGroup 추가

			go func(buildings []map[string]string) {
				batchByThousand(buildings, basic, counter)
				wg.Done() // WaitGroup 완료 시그널 전송
			}(buildings)

			wg.Wait() // 모든 batchByThousand 함수가 완료될 때까지 대기
			buildings = buildings[:0]
		}
	}

	// 나머지
	if len(buildings) > 0 {
		wg.Add(1) // WaitGroup 추가

		go func(buildings []map[string]string) {
			batchByThousand(buildings, basic, counter)
			wg.Done() // WaitGroup 완료 시그널 전송
		}(buildings)
	}

}

func batchByThousand(data []map[string]string, basic dbHandler.TableBasics, counter int) {
	fmt.Println("batchByThousand working...", counter)
	var wg sync.WaitGroup
	defer wg.Wait()
	semaphore := make(chan struct{}, 2300)
	for i := 0; i < 2300*25; i += 25 {
		wg.Add(1)
		semaphore <- struct{}{}
		i := i
		go func(i int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			basic.AddBuildings(data[i : i+25])
		}(i)
	}
}
