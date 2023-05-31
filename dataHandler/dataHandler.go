package dataHandler

import "strings"

// SplitByString 주어진 문자열을 pipeline 을 기준으로 자른 배열을 반환하는 함수
func SplitByString(origin, separator string) []string {
	return strings.Split(origin, separator)
}
