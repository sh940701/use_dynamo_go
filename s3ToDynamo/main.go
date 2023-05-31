package main

import (
	"api-public-data/s3ToDynamo/dataHandler"
	"api-public-data/s3ToDynamo/dbHandler"
	"bufio"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"sync"
	"time"
)

func main() {
	start := time.Now()

	// AWS 세션 생성
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load AWS configuration: %v", err)
	}

	// S3 클라이언트 생성
	s3Client := s3.NewFromConfig(cfg)

	// DynamoDB 클라이언트 생성
	dynamoDBClient := dynamodb.NewFromConfig(cfg)

	// S3 버킷 및 파일 정보 설정
	bucket := "testbuildingbucket"
	key := "mart_djy_03.txt"

	// 텍스트 라인 별로 DynamoDB에 저장
	err = uploadLinesToDynamoDB(context.TODO(), s3Client, dynamoDBClient, "building1", bucket, key)
	if err != nil {
		log.Fatalf("failed to upload lines to DynamoDB: %v", err)
	}

	fmt.Println("Data upload to DynamoDB completed successfully.")

	duration := time.Since(start)
	fmt.Println("실행시간: ", duration)
}

// 텍스트 라인 별로 DynamoDB에 저장
func uploadLinesToDynamoDB(ctx context.Context, s3Client *s3.Client, dynamoDBClient *dynamodb.Client, tableName, bucket, key string) error {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	// 한 번에 write 할 수 있는 최대 개수
	limit := 25

	// S3 파일 스트림 가져오기
	resp, err := s3Client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	const maxLinesPerBatch = 25

	var lines []string
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 20)

	for scanner.Scan() {
		// line 단위로 byte 를 읽어온다.
		byteLine := scanner.Bytes()

		// byte 를 인코딩 후 string 으로 변경
		str := dataHandler.EncodeBytes(byteLine)

		lines = append(lines, str)

		if len(lines) == limit {
			linesCopy := make([]string, len(lines))
			copy(linesCopy, lines)

			wg.Add(1)
			semaphore <- struct{}{}

			go func(data []string) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				var documentPool [][]string
				for _, item := range data {
					documentPool = append(documentPool, dataHandler.SplitByPipeline(item))
				}

				documents := dataHandler.ToDynamoInsertType(documentPool)

				dbHandler.AddBuildings(documents, dynamoDBClient, tableName)
				if err != nil {
					fmt.Errorf("failed to put remaining items to DynamoDB: %v", err)
				}
			}(linesCopy)

			lines = lines[:0]
		}

	}

	if len(lines) > 0 {
		var documentPool [][]string
		for _, item := range lines {
			documentPool = append(documentPool, dataHandler.SplitByPipeline(item))
		}

		documents := dataHandler.ToDynamoInsertType(documentPool)
		err := dbHandler.AddBuildings(documents, dynamoDBClient, tableName)
		if err != nil {
			return fmt.Errorf("failed to put remaining items to DynamoDB: %v", err)
		}
	}

	wg.Wait() // 모든 Goroutine 이 종료될 때까지 대기

	return nil
}
