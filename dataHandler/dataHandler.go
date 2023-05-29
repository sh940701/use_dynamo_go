package dataHandler

import "strings"

// SplitByPipeline 주어진 문자열을 pipeline 을 기준으로 자른 배열을 반환하는 함수
func SplitByPipeline(origin string) []string {
	return strings.Split(origin, "|")
}
